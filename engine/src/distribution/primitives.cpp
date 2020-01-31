#include "distribution/primitives.h"
#include "CalciteExpressionParsing.h"
#include "ColumnManipulation.cuh"
#include "Traits/RuntimeTraits.h"
#include "communication/CommunicationData.h"
#include "communication/factory/MessageFactory.h"
#include "communication/messages/ComponentMessages.h"
#include "communication/network/Client.h"
#include "communication/network/Server.h"
#include "config/GPUManager.cuh"
#include "cuDF/generator/sample_generator.h"
#include "cuDF/safe_nvcategory_gather.hpp"
#include "distribution/Exception.h"
#include "distribution/primitives_util.cuh"
#include "legacy/groupby.hpp"
#include "legacy/reduction.hpp"
#include "operators/GroupBy.h"
#include "utilities/RalColumn.h"
#include "utilities/StringUtils.h"
#include "utilities/TableWrapper.h"
#include <algorithm>
#include <blazingdb/io/Library/Logging/Logger.h>
#include <cassert>
#include <cmath>
#include <cudf/legacy/table.hpp>
#include <iostream>
#include <memory>
#include <numeric>
#include <types.hpp>

#include "cudf/legacy/copying.hpp"
#include "cudf/legacy/merge.hpp"
#include "cudf/legacy/search.hpp"
#include "cudf/search.hpp"
#include <cudf/sorting.hpp>
#include <cudf/copying.hpp>
#include <cudf/merge.hpp>
#include <from_cudf/cpp_tests/utilities/column_wrapper.hpp>
#include <from_cudf/cpp_tests/utilities/column_utilities.hpp>

#include "utilities/CommonOperations.h"

namespace ral {
namespace distribution {
namespace sampling {

double calculateSampleRatio(cudf::size_type tableSize) { return std::ceil(1.0 - std::pow(tableSize / 1.0E11, 8E-4)); }

std::vector<gdf_column_cpp> generateSample(const std::vector<gdf_column_cpp> & table, double ratio) {
	std::size_t quantity = std::ceil(table[0].get_gdf_column()->size() * ratio);
	return generateSample(table, quantity);
}

std::vector<std::vector<gdf_column_cpp>> generateSamples(
	const std::vector<std::vector<gdf_column_cpp>> & tables, const std::vector<double> & ratios) {
	std::vector<std::size_t> quantities;
	quantities.reserve(tables.size());

	for(std::size_t i = 0; i < tables.size(); i++) {
		quantities.push_back(std::ceil(tables[i][0].get_gdf_column()->size() * ratios[i]));
	}

	return generateSamples(tables, quantities);
}

std::vector<gdf_column_cpp> generateSample(const std::vector<gdf_column_cpp> & table, std::size_t quantity) {
	std::vector<gdf_column_cpp> sample;

	gdf_error gdf_status = cudf::generator::generate_sample(table, sample, quantity);
	if(GDF_SUCCESS != gdf_status) {
		throw std::runtime_error(
			"[ERROR] " + std::string{__FUNCTION__} + " -- CUDF: " + gdf_error_get_name(gdf_status));
	}

	return sample;
}

std::vector<std::vector<gdf_column_cpp>> generateSamples(
	const std::vector<std::vector<gdf_column_cpp>> & input_tables, std::vector<std::size_t> & quantities) {
	// verify
	if(input_tables.size() != quantities.size()) {
		throw std::runtime_error("[ERROR] " + std::string{__FUNCTION__} + " -- size mismatch.");
	}

	// output data
	std::vector<std::vector<gdf_column_cpp>> result;

	// make sample for each table
	for(std::size_t k = 0; k < input_tables.size(); ++k) {
		result.emplace_back(generateSample(input_tables[k], quantities[k]));
	}

	// done
	return result;
}

void normalizeSamples(std::vector<NodeSamples> & samples) {
	std::vector<double> representativities(samples.size());

	for(std::size_t i = 0; i < samples.size(); i++) {
		representativities[i] = (double) samples[i].getColumns()[0].get_gdf_column()->size() / samples[i].getTotalRowSize();
	}

	const double minimumRepresentativity = *std::min_element(representativities.cbegin(), representativities.cend());

	for(std::size_t i = 0; i < samples.size(); i++) {
		double representativenessRatio = minimumRepresentativity / representativities[i];

		if(representativenessRatio > THRESHOLD_FOR_SUBSAMPLING) {
			samples[i].setColumns(generateSample(samples[i].getColumns(), representativenessRatio));
		}
	}
}

}  // namespace sampling
}  // namespace distribution
}  // namespace ral

namespace ral {
namespace distribution {
namespace experimental {

typedef ral::frame::BlazingTable BlazingTable;
typedef ral::frame::BlazingTableView BlazingTableView;
typedef blazingdb::manager::experimental::Context Context;
typedef blazingdb::transport::experimental::Node Node;
typedef ral::communication::messages::experimental::Factory Factory;
typedef ral::communication::messages::experimental::SampleToNodeMasterMessage SampleToNodeMasterMessage;
typedef ral::communication::messages::experimental::PartitionPivotsMessage PartitionPivotsMessage;
typedef ral::communication::messages::experimental::ColumnDataMessage ColumnDataMessage;
typedef ral::communication::messages::experimental::GPUComponentReceivedMessage GPUComponentReceivedMessage;
typedef ral::communication::experimental::CommunicationData CommunicationData;
typedef ral::communication::network::experimental::Server Server;
typedef ral::communication::network::experimental::Client Client;

std::vector<NodeColumns> generateJoinPartitions(
	const Context & context, std::vector<gdf_column_cpp> & table, std::vector<int> & columnIndices) {
	assert(table.size() != 0);

	if(table[0].get_gdf_column()->size() == 0) {
		std::vector<NodeColumns> result;
		auto nodes = context.getAllNodes();
		for(cudf::size_type k = 0; k < nodes.size(); ++k) {
			std::vector<gdf_column_cpp> columns = table;
			// TODO percy william felipe port distribution cudf0.12
			//result.emplace_back(*nodes[k], columns);
		}
		return result;
	}

	// Support for GDF_STRING_CATEGORY. We need the string hashes not the category indices
	std::vector<gdf_column_cpp> temp_input_table(table);
	std::vector<int> temp_input_col_indices(columnIndices);
	for(size_t i = 0; i < columnIndices.size(); i++) {
		gdf_column_cpp & col = table[columnIndices[i]];

		if(col.get_gdf_column()->type().id() != cudf::type_id::STRING)
			continue;

		// TODO percy cudf0.12 port to cudf::column and custrings
//		NVCategory * nvCategory = static_cast<NVCategory *>(col.get_gdf_column()->dtype_info.category);
//		NVStrings * nvStrings =
//			nvCategory->gather_strings(static_cast<nv_category_index_type *>(col.data()), col.get_gdf_column()->size(), true);

//		gdf_column_cpp str_hash;
//		str_hash.create_gdf_column(cudf::type_id::INT32,
//			nvStrings->size(),
//			nullptr,
//			nullptr,
//			ral::traits::get_dtype_size_in_bytes(cudf::type_id::INT32),
//			"");
//		nvStrings->hash(static_cast<unsigned int *>(str_hash.data()));
//		NVStrings::destroy(nvStrings);

//		temp_input_col_indices[i] = temp_input_table.size();
//		temp_input_table.push_back(str_hash);
	}


	std::vector<cudf::column *> raw_input_table_col_ptrs(temp_input_table.size());
	std::transform(
		temp_input_table.begin(), temp_input_table.end(), raw_input_table_col_ptrs.begin(), [](auto & cpp_col) {
			return cpp_col.get_gdf_column();
		});
	
	// TODO percy cudf0.12 port to cudf::column
	//cudf::table input_table_wrapper(raw_input_table_col_ptrs);

	// Generate partition offset vector
	cudf::size_type number_nodes = context.getTotalNodes();
	std::vector<gdf_size_type> partition_offset(number_nodes);

	// Preallocate output columns
	// TODO percy cudf0.12 port to cudf::column
	//std::vector<gdf_column_cpp> output_columns = generateOutputColumns(input_table_wrapper.num_columns(), input_table_wrapper.num_rows(), temp_input_table);

	// copy over nvcategory to output
	// NOTE that i am having to do this due to an already identified issue: https://github.com/rapidsai/cudf/issues/1474
	// TODO percy cudf0.12 port to cudf::column
//	for(size_t i = 0; i < output_columns.size(); i++) {
//		// TODO percy cudf0.12 custrings this was not commented
////		if(output_columns[i].dtype() == GDF_STRING_CATEGORY && temp_input_table[i].dtype_info().category) {
////			output_columns[i].get_gdf_column()->dtype_info.category =
////				static_cast<void *>(static_cast<NVCategory *>(temp_input_table[i].dtype_info().category)->copy());
////		}
//	}

	// TODO percy cudf0.12 port to cudf::column
//	std::vector<gdf_column *> raw_output_table_col_ptrs(output_columns.size());
//	std::transform(output_columns.begin(), output_columns.end(), raw_output_table_col_ptrs.begin(), [](auto & cpp_col) {
//		return cpp_col.get_gdf_column();
//	});
//	cudf::table output_table_wrapper(raw_output_table_col_ptrs);

	// Execute operation
	// TODO percy cudf0.12 port to cudf::column
//	CUDF_CALL(gdf_hash_partition(input_table_wrapper.num_columns(),
//		input_table_wrapper.begin(),
//		temp_input_col_indices.data(),
//		temp_input_col_indices.size(),
//		number_nodes,
//		output_table_wrapper.begin(),
//		partition_offset.data(),
//		gdf_hash_func::GDF_HASH_MURMUR3));

	// TODO percy cudf0.12 port to cudf::column
//	std::vector<gdf_column_cpp> temp_output_columns(output_columns.begin(), output_columns.begin() + table.size());
//	for(size_t i = 0; i < table.size(); i++) {
//		gdf_column * srcCol = input_table_wrapper.get_column(i);
//		gdf_column * dstCol = output_table_wrapper.get_column(i);

//		if(srcCol->dtype != GDF_STRING_CATEGORY)
//			continue;

//		ral::safe_nvcategory_gather_for_string_category(dstCol, srcCol->dtype_info.category);
//	}

	// Erase input table
	table.clear();

	// lets get the split indices. These are all the partition_offset, except for the first since its just 0
	gdf_column_cpp indexes;
	indexes.create_gdf_column(cudf::type_id::INT32,
		number_nodes - 1,
		partition_offset.data() + 1,
		nullptr,
		ral::traits::get_dtype_size_in_bytes(cudf::type_id::INT32),
		"");

	// TODO percy cudf0.12 port to cudf::column
	//return split_data_into_NodeColumns(context, temp_output_columns, indexes);
}

void sendSamplesToMaster(Context * context, const BlazingTableView & samples, std::size_t table_total_rows) {
  // Get master node
	const Node & master_node = context->getMasterNode();

	// Get self node
	Node self_node = CommunicationData::getInstance().getSelfNode();

	// Get context token
	const uint32_t context_comm_token = context->getContextCommunicationToken();
	const uint32_t context_token = context->getContextToken();
	const std::string message_id = SampleToNodeMasterMessage::MessageID() + "_" + std::to_string(context_comm_token);

	auto message =
		Factory::createSampleToNodeMaster(message_id, context_token, self_node, table_total_rows, samples);

	// // Send message to master
	// using Client = ral::communication::network::experimental::Client;
	// Send message to master
	// WSM TODO cudf0.12

	// Library::Logging::Logger().logTrace(ral::utilities::buildLogString(std::to_string(context_token),
	// 	std::to_string(context->getQueryStep()),
	// 	std::to_string(context->getQuerySubstep()),
	// 	"About to send sendSamplesToMaster message"));
	Client::send(master_node, *message);
}

std::pair<std::vector<NodeColumn>, std::vector<std::size_t> > collectSamples(Context * context) {

	const uint32_t context_comm_token = context->getContextCommunicationToken();
	const uint32_t context_token = context->getContextToken();
	const std::string message_id = SampleToNodeMasterMessage::MessageID() + "_" + std::to_string(context_comm_token);

	std::vector<NodeColumn> nodeColumns;
	std::vector<std::size_t> table_total_rows;

	size_t size = context->getWorkerNodes().size();
	std::vector<bool> received(context->getTotalNodes(), false);
	for(int k = 0; k < size; ++k) {
		auto message = Server::getInstance().getMessage(context_token, message_id);

		if(message->getMessageTokenValue() != message_id) {
			throw createMessageMismatchException(__FUNCTION__, message_id, message->getMessageTokenValue());
		}

		auto node = message->getSenderNode();
		int node_idx = context->getNodeIndex(node);
		if(received[node_idx]) {
			Library::Logging::Logger().logError(ral::utilities::buildLogString(std::to_string(context_token),
				std::to_string(context->getQueryStep()),
				std::to_string(context->getQuerySubstep()),
				"ERROR: Already received collectSamples from node " + std::to_string(node_idx)));
		}
		auto concreteMessage = std::static_pointer_cast<GPUComponentReceivedMessage>(message);
		table_total_rows.push_back(concreteMessage->getTotalRowSize());
		nodeColumns.emplace_back(std::make_pair(node, std::move(concreteMessage->releaseBlazingTable())));
		received[node_idx] = true;
	}

	return std::make_pair(std::move(nodeColumns), table_total_rows);
}


std::unique_ptr<BlazingTable> generatePartitionPlans(
				Context * context, std::vector<BlazingTableView> & samples,
				const std::vector<std::size_t> & table_total_rows, const std::vector<int8_t> & sortOrderTypes) {

	std::unique_ptr<BlazingTable> concatSamples = ral::utilities::experimental::concatTables(samples);

	std::vector<cudf::order> column_order;
	for(auto col_order : sortOrderTypes){
		if(col_order)
			column_order.push_back(cudf::order::DESCENDING);
		else
			column_order.push_back(cudf::order::ASCENDING);
	}
	std::vector<cudf::null_order> null_orders(column_order.size(), cudf::null_order::AFTER);
	// TODO this is just a default setting. Will want to be able to properly set null_order
	std::unique_ptr<cudf::column> sort_indices = cudf::experimental::sorted_order( concatSamples->view(), column_order, null_orders);

	std::unique_ptr<CudfTable> sortedSamples = cudf::experimental::gather( concatSamples->view(), sort_indices->view() );

	// lets get names from a non-empty table
	std::vector<std::string> names;
	for(size_t i = 0; i < samples.size(); i++) {
		if (samples[i].names().size() > 0){
			names = samples[i].names();
			break;
		}
	}

	return getPivotPointsTable(context, BlazingTableView(sortedSamples->view(), names));
}

void distributePartitionPlan(Context * context, const BlazingTableView & pivots) {

	const uint32_t context_comm_token = context->getContextCommunicationToken();
	const uint32_t context_token = context->getContextToken();
	const std::string message_id = PartitionPivotsMessage::MessageID() + "_" + std::to_string(context_comm_token);

	auto node = CommunicationData::getInstance().getSelfNode();
	auto message = Factory::createPartitionPivotsMessage(message_id, context_token, node, pivots);
	broadcastMessage(context->getWorkerNodes(), message);
}

std::unique_ptr<BlazingTable> getPartitionPlan(Context * context) {

	const uint32_t context_comm_token = context->getContextCommunicationToken();
	const uint32_t context_token = context->getContextToken();
	const std::string message_id = PartitionPivotsMessage::MessageID() + "_" + std::to_string(context_comm_token);

	auto message = Server::getInstance().getMessage(context_token, message_id);

	if(message->getMessageTokenValue() != message_id) {
		throw createMessageMismatchException(__FUNCTION__, message_id, message->getMessageTokenValue());
	}

	auto concreteMessage = std::static_pointer_cast<GPUComponentReceivedMessage>(message);
	return concreteMessage->releaseBlazingTable();
}


// This function locates the pivots in the table and partitions the data on those pivot points.
// IMPORTANT: This function expects data to aready be sorted according to the searchColIndices and sortOrderTypes
// IMPORTANT: The TableViews of the data returned point to the same data that was input.
std::vector<NodeColumnView> partitionData(Context * context,
											const BlazingTableView & table,
											const BlazingTableView & pivots,
											const std::vector<int> & searchColIndices,
											std::vector<int8_t> sortOrderTypes) {

	// verify input
	if(pivots.view().num_columns() == 0) {
		throw std::runtime_error("The pivots array is empty");
	}
	if(pivots.view().num_columns() != searchColIndices.size()) {
		throw std::runtime_error("The pivots and searchColIndices vectors don't have the same size");
	}

	cudf::size_type num_rows = table.view().num_rows();
	if(num_rows == 0) {
		std::vector<NodeColumnView> array_node_columns;
		auto nodes = context->getAllNodes();
		for(std::size_t i = 0; i < nodes.size(); ++i) {
			array_node_columns.emplace_back(nodes[i], BlazingTableView(table.view(), table.names()));
		}
		return array_node_columns;
	}

	if(sortOrderTypes.size() == 0) {
		sortOrderTypes.assign(searchColIndices.size(), 0);
	}

	std::vector<cudf::order> column_order;
	for(auto col_order : sortOrderTypes){
		if(col_order)
			column_order.push_back(cudf::order::DESCENDING);
		else
			column_order.push_back(cudf::order::ASCENDING);
	}
	// TODO this is just a default setting. Will want to be able to properly set null_order
	std::vector<cudf::null_order> null_orders(column_order.size(), cudf::null_order::AFTER);

	CudfTableView columns_to_search = table.view().select(searchColIndices);

	std::unique_ptr<cudf::column> pivot_indexes = cudf::experimental::upper_bound(columns_to_search,
                                    pivots.view(),
                                    column_order,
                                    null_orders);

	std::pair<std::vector<cudf::size_type>, std::vector<cudf::bitmask_type>> host_pivot_indexes = cudf::test::to_host<cudf::size_type>(pivot_indexes->view());

	std::vector<CudfTableView> partitioned_data = cudf::experimental::split(table.view(), host_pivot_indexes.first);

	std::unique_ptr<CudfTable> empty;
	if (partitioned_data.size() == host_pivot_indexes.first ){ // split can return one less, due to weird implementation details in cudf. This only happens if the last one should be empty
		empty = cudf::experimental::empty_like(table.view());
		partitioned_data.emplace_back(empty->view());
	}

	std::vector<Node> all_nodes = context->getAllNodes();

	if(all_nodes.size() != partitioned_data.size()){
		std::string err = "Number of CudfTableView from partitionData does not match number of nodes";
		Library::Logging::Logger().logError(ral::utilities::buildLogString(std::to_string(context->getContextToken()), std::to_string(context->getQueryStep()), std::to_string(context->getQuerySubstep()), err));
	}
	std::vector<NodeColumnView> partitioned_node_column_views;
	for (int i = 0; i < all_nodes.size(); i++){
		partitioned_node_column_views.push_back(std::make_pair(all_nodes[i], BlazingTableView(partitioned_data[i], table.names())));
	}
	return partitioned_node_column_views;

}

void distributePartitions(Context * context, std::vector<NodeColumnView> & partitions) {

	const uint32_t context_comm_token = context->getContextCommunicationToken();
	const uint32_t context_token = context->getContextToken();
	const std::string message_id = ColumnDataMessage::MessageID() + "_" + std::to_string(context_comm_token);

	auto self_node = CommunicationData::getInstance().getSelfNode();
	std::vector<std::thread> threads;
	for(auto & nodeColumn : partitions) {
		if(nodeColumn.first == self_node) {
			continue;
		}
		BlazingTableView columns = nodeColumn.second;
		auto destination_node = nodeColumn.first;
		threads.push_back(std::thread([message_id, context_token, self_node, destination_node, columns]() mutable {
			auto message = Factory::createColumnDataMessage(message_id, context_token, self_node, columns);
			Client::send(destination_node, *message);
		}));
	}
	for(size_t i = 0; i < threads.size(); i++) {
		threads[i].join();
	}
}

std::vector<NodeColumn> collectPartitions(Context * context) {
	int num_partitions = context->getTotalNodes() - 1;
	return collectSomePartitions(context, num_partitions);
}

std::vector<NodeColumn> collectSomePartitions(Context * context, int num_partitions) {

	// Get the numbers of rals in the query
	int number_rals = context->getTotalNodes() - 1;
	std::vector<bool> received(context->getTotalNodes(), false);

	// Create return value
	std::vector<NodeColumn> node_columns;

	// Get message from the server
	const uint32_t context_comm_token = context->getContextCommunicationToken();
	const uint32_t context_token = context->getContextToken();
	const std::string message_id = ColumnDataMessage::MessageID() + "_" + std::to_string(context_comm_token);

	while(0 < num_partitions) {
		auto message = Server::getInstance().getMessage(context_token, message_id);
		num_partitions--;

		if(message->getMessageTokenValue() != message_id) {
			throw createMessageMismatchException(__FUNCTION__, message_id, message->getMessageTokenValue());
		}

		auto node = message->getSenderNode();
		int node_idx = context->getNodeIndex(node);
		if(received[node_idx]) {
			Library::Logging::Logger().logError(ral::utilities::buildLogString(std::to_string(context_token),
				std::to_string(context->getQueryStep()),
				std::to_string(context->getQuerySubstep()),
				"ERROR: Already received collectSomePartitions from node " + std::to_string(node_idx)));
		}
		auto concreteMessage = std::static_pointer_cast<GPUComponentReceivedMessage>(message);
		node_columns.emplace_back(std::make_pair(node, std::move(concreteMessage->releaseBlazingTable())));
		received[node_idx] = true;
	}
	return node_columns;
}

void scatterData(Context * context, const BlazingTableView & table) {

	std::vector<NodeColumnView> node_columns;
	auto nodes = context->getAllNodes();
	for(std::size_t i = 0; i < nodes.size(); ++i) {
		if(!(nodes[i] == CommunicationData::getInstance().getSelfNode())) {
			node_columns.emplace_back(nodes[i], table);
		}
	}
	distributePartitions(context, node_columns);
}

std::unique_ptr<BlazingTable> sortedMerger(std::vector<BlazingTableView> & tables,
	const std::vector<int8_t> & sortOrderTypes,
	const std::vector<int> & sortColIndices) {

	std::vector<cudf::order> column_order;
	for(auto col_order : sortOrderTypes){
		if(col_order)
			column_order.push_back(cudf::order::DESCENDING);
		else
			column_order.push_back(cudf::order::ASCENDING);
	}
	// TODO this is just a default setting. Will want to be able to properly set null_order
	std::vector<cudf::null_order> null_orders(column_order.size(), cudf::null_order::AFTER);

	std::unique_ptr<CudfTable> merged_table;
	CudfTableView left_table = tables[0].view();
	for(size_t i = 1; i < tables.size(); i++) {

		CudfTableView right_table = tables[i].view();

		merged_table = cudf::experimental::merge(left_table, right_table,
													sortColIndices, column_order, null_orders);

		left_table = merged_table->view();
	}

	// lets get names from a non-empty table
	std::vector<std::string> names;
	for(size_t i = 0; i < tables.size(); i++) {
		if (tables[i].names().size() > 0){
			names = tables[i].names();
			break;
		}
	}
	return std::make_unique<BlazingTable>(std::move(merged_table), names);
}


std::unique_ptr<BlazingTable> getPivotPointsTable(Context * context, const BlazingTableView & sortedSamples){
	cudf::size_type outputRowSize = sortedSamples.view().num_rows();
	cudf::size_type pivotsSize = outputRowSize > 0 ? context->getTotalNodes() - 1 : 0;

	int32_t step = outputRowSize / context->getTotalNodes();

	auto sequence_iter = cudf::test::make_counting_transform_iterator(0, [step](auto i) { return int32_t(i * step) + step;});
	cudf::test::fixed_width_column_wrapper<int32_t> gather_map_wrapper(sequence_iter, sequence_iter + pivotsSize);
	CudfColumnView gather_map(gather_map_wrapper);
	std::unique_ptr<CudfTable> pivots = cudf::experimental::gather( sortedSamples.view(), gather_map );

	return std::make_unique<BlazingTable>(std::move(pivots), sortedSamples.names());
}


std::unique_ptr<BlazingTable> generatePartitionPlansGroupBy(Context * context, std::vector<BlazingTableView> & samples) {

	std::unique_ptr<BlazingTable> concatSamples = ral::utilities::experimental::concatTables(samples);

	std::vector<int> groupColumnIndices(concatSamples->view().num_columns());
	std::iota(groupColumnIndices.begin(), groupColumnIndices.end(), 0);
	std::unique_ptr<BlazingTable> groupedSamples = ral::operators::experimental::compute_groupby_without_aggregations(
														concatSamples->toBlazingTableView(), groupColumnIndices);

	// Sort
	std::vector<cudf::order> column_order(groupedSamples->view().num_columns(), cudf::order::ASCENDING);
	std::vector<cudf::null_order> null_orders(column_order.size(), cudf::null_order::AFTER);
	std::unique_ptr<cudf::column> sort_indices = cudf::experimental::sorted_order( groupedSamples->view(), column_order, null_orders);
	std::unique_ptr<CudfTable> sortedSamples = cudf::experimental::gather( groupedSamples->view(), sort_indices->view() );

	// lets get names from a non-empty table
	std::vector<std::string> names;
	for(size_t i = 0; i < samples.size(); i++) {
		if (samples[i].names().size() > 0){
			names = samples[i].names();
			break;
		}
	}

	return getPivotPointsTable(context, BlazingTableView(sortedSamples->view(), names));
}

std::unique_ptr<BlazingTable> groupByWithoutAggregationsMerger(
	std::vector<BlazingTableView> & tables, const std::vector<int> & group_column_indices) {
	
	std::unique_ptr<BlazingTable> concatGroups = ral::utilities::experimental::concatTables(tables);

	return ral::operators::experimental::compute_groupby_without_aggregations(concatGroups->toBlazingTableView(),  group_column_indices);	
}

void broadcastMessage(std::vector<Node> nodes, 
			std::shared_ptr<communication::messages::experimental::Message> message) {
	std::vector<std::thread> threads(nodes.size());
	for(size_t i = 0; i < nodes.size(); i++) {
		Node node = nodes[i];
		threads[i] = std::thread([node, message]() {
			Client::send(node, *message);
		});
	}
	for(size_t i = 0; i < threads.size(); i++) {
		threads[i].join();
	}
}

void distributeNumRows(Context * context, cudf::size_type num_rows) {
	
	const uint32_t context_comm_token = context->getContextCommunicationToken();
	const uint32_t context_token = context->getContextToken();
	const std::string message_id = SampleToNodeMasterMessage::MessageID() + "_" + std::to_string(context_comm_token);

	auto self_node = CommunicationData::getInstance().getSelfNode();
	auto message = Factory::createSampleToNodeMaster(message_id, context_token, self_node, num_rows, {});

	int self_node_idx = context->getNodeIndex(CommunicationData::getInstance().getSelfNode());
	broadcastMessage(context->getAllOtherNodes(self_node_idx), message);
}

std::vector<cudf::size_type> collectNumRows(Context * context) {
	
	int num_nodes = context->getTotalNodes();
	std::vector<cudf::size_type> node_num_rows(num_nodes);
	std::vector<bool> received(num_nodes, false);

	const uint32_t context_comm_token = context->getContextCommunicationToken();
	const uint32_t context_token = context->getContextToken();
	const std::string message_id = SampleToNodeMasterMessage::MessageID() + "_" + std::to_string(context_comm_token);

	int self_node_idx = context->getNodeIndex(CommunicationData::getInstance().getSelfNode());
	for(cudf::size_type i = 0; i < num_nodes - 1; ++i) {
		auto message = Server::getInstance().getMessage(context_token, message_id);
		if(message->getMessageTokenValue() != message_id) {
			throw createMessageMismatchException(__FUNCTION__, message_id, message->getMessageTokenValue());
		}
		auto concrete_message = std::static_pointer_cast<GPUComponentReceivedMessage>(message);
		auto node = concrete_message->getSenderNode();
		int node_idx = context->getNodeIndex(node);
		assert(node_idx >= 0);
		if(received[node_idx]) {
			Library::Logging::Logger().logError(ral::utilities::buildLogString(std::to_string(context_token),
				std::to_string(context->getQueryStep()),
				std::to_string(context->getQuerySubstep()),
				"ERROR: Already received collectRowSize from node " + std::to_string(node_idx)));
		}
		node_num_rows[node_idx] = concrete_message->getTotalRowSize();
		received[node_idx] = true;
	}

	return node_num_rows;
}

void distributeLeftRightNumRows(Context * context, std::size_t left_num_rows, std::size_t right_num_rows) {
	
	const uint32_t context_comm_token = context->getContextCommunicationToken();
	const uint32_t context_token = context->getContextToken();
	const std::string message_id = SampleToNodeMasterMessage::MessageID() + "_" + std::to_string(context_comm_token);

	auto self_node = CommunicationData::getInstance().getSelfNode();
	cudf::test::fixed_width_column_wrapper<cudf::size_type>num_rows_col{left_num_rows, right_num_rows};
	CudfTableView num_rows_table{{num_rows_col}};
	std::vector<std::string> names{"left_num_rows", "right_num_rows"};
	BlazingTableView num_rows_blz_table(num_rows_table, names);
	auto message = Factory::createSampleToNodeMaster(message_id, context_token, self_node, 0, num_rows_blz_table);

	int self_node_idx = context->getNodeIndex(CommunicationData::getInstance().getSelfNode());
	broadcastMessage(context->getAllOtherNodes(self_node_idx), message);
}

void collectLeftRightNumRows(Context * context,	std::vector<cudf::size_type> & node_num_rows_left,
			std::vector<cudf::size_type> & node_num_rows_right) {
	
	int num_nodes = context->getTotalNodes();
	node_num_rows_left.resize(num_nodes);
	node_num_rows_right.resize(num_nodes);
	std::vector<bool> received(num_nodes, false);

	const uint32_t context_comm_token = context->getContextCommunicationToken();
	const uint32_t context_token = context->getContextToken();
	const std::string message_id = SampleToNodeMasterMessage::MessageID() + "_" + std::to_string(context_comm_token);

	int self_node_idx = context->getNodeIndex(CommunicationData::getInstance().getSelfNode());
	for(cudf::size_type i = 0; i < num_nodes - 1; ++i) {
		auto message = Server::getInstance().getMessage(context_token, message_id);
		if(message->getMessageTokenValue() != message_id) {
			throw createMessageMismatchException(__FUNCTION__, message_id, message->getMessageTokenValue());
		}
		auto concrete_message = std::static_pointer_cast<GPUComponentReceivedMessage>(message);
		auto node = concrete_message->getSenderNode();
		std::unique_ptr<BlazingTable> num_rows_data = concrete_message->releaseBlazingTable();
		assert(num_rows_data->view().num_columns() == 1);
		assert(num_rows_data->view().num_rows() == 2);
		
		std::pair<std::vector<cudf::size_type>, std::vector<cudf::bitmask_type>> num_rows_host = cudf::test::to_host<cudf::size_type>(num_rows_data->view().column(0));
		
		int node_idx = context->getNodeIndex(node);
		assert(node_idx >= 0);
		if(received[node_idx]) {
			Library::Logging::Logger().logError(ral::utilities::buildLogString(std::to_string(context_token),
				std::to_string(context->getQueryStep()),
				std::to_string(context->getQuerySubstep()),
				"ERROR: Already received collectLeftRightNumRows from node " + std::to_string(node_idx)));
		}
		node_num_rows_left[node_idx] = num_rows_host.first[0];
		node_num_rows_right[node_idx] = num_rows_host.first[1];
		received[node_idx] = true;
	}
}

}  // namespace experimental
}  // namespace distribution
}  // namespace ral



namespace ral {
namespace distribution {
namespace sampling {
namespace experimental {

std::unique_ptr<ral::frame::BlazingTable> generateSamples(
	const ral::frame::BlazingTableView & table, const double ratio) {

	return generateSamples(table, std::ceil(table.view().num_rows() * ratio));
}

std::unique_ptr<ral::frame::BlazingTable> generateSamples(
	const ral::frame::BlazingTableView & table, const size_t quantile) {

	return cudf::generator::generate_sample(table, quantile);
}

}  // namespace experimental
}  // namespace sampling
}  // namespace distribution
}  // namespace ral
