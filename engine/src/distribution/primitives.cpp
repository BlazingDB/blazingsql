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
typedef ral::communication::experimental::CommunicationData CommunicationData;
typedef ral::communication::network::experimental::Server Server;
typedef ral::communication::network::experimental::Client Client;



// namespace Factory = ral::communication::messages::Factory;
// namespace SampleToNodeMasterMessage = ral::communication::messages::experimental::SampleToNodeMasterMessage;
// namespace PartitionPivotsMessage = ral::communication::messages::experimental::PartitionPivotsMessage;
// namespace CommunicationData = ral::communication::experimental::CommunicationData;
// namespace Server = ral::communication::network::experimental::Server;
// namespace Client = ral::communication::network::experimental::Client;

void sendSamplesToMaster(Context * context, const BlazingTableView & samples, std::size_t table_total_rows) {
	
	// Get master node
	const Node & master_node = context->getMasterNode();

	// Get self node	
	Node self_node = CommunicationData::getInstance().getSelfNode();

	// Get context token
	const uint32_t context_comm_token = context->getContextCommunicationToken();
	const uint32_t context_token = context->getContextToken();
	const std::string message_id = SampleToNodeMasterMessage::MessageID() + "_" + std::to_string(context_comm_token);

	// WSM TODO cudf0.12 waiting on message Node refactor
	// auto message =
	// 	Factory::createSampleToNodeMaster(message_id, context_token, self_node, total_row_size, samples);

	// // Send message to master
	// using Client = ral::communication::network::experimental::Client;
	// Library::Logging::Logger().logTrace(ral::utilities::buildLogString(std::to_string(context_token),
	// 	std::to_string(context->getQueryStep()),
	// 	std::to_string(context->getQuerySubstep()),
	// 	"About to send sendSamplesToMaster message"));
	// Client::send(master_node, *message);
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

		// WSM TODO cudf0.12
		// auto concreteMessage = std::static_pointer_cast<SampleToNodeMasterMessage>(message);
		// auto node = message->getSenderNode();
		// int node_idx = context->getNodeIndex(node);
		// if(received[node_idx]) {
		// 	Library::Logging::Logger().logError(ral::utilities::buildLogString(std::to_string(context_token),
		// 		std::to_string(context->getQueryStep()),
		// 		std::to_string(context->getQuerySubstep()),
		// 		"ERROR: Already received collectSamples from node " + std::to_string(node_idx)));
		// }
		// table_total_rows.push_back(concreteMessage->getTotalRowSize());
		// nodeColumns.emplace_back(std::make_pair(node, std::move(concreteMessage->getSamples()));
		// received[node_idx] = true;
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
	// WSM TODO wating on message
	// auto message = Factory::createPartitionPivotsMessage(message_id, context_token, node, pivots);
	// broadcastMessage(context->getWorkerNodes(), message);
}

std::unique_ptr<BlazingTable> getPartitionPlan(Context * context) {
	
	const uint32_t context_comm_token = context->getContextCommunicationToken();
	const uint32_t context_token = context->getContextToken();
	const std::string message_id = PartitionPivotsMessage::MessageID() + "_" + std::to_string(context_comm_token);

	auto message = Server::getInstance().getMessage(context_token, message_id);

	if(message->getMessageTokenValue() != message_id) {
		throw createMessageMismatchException(__FUNCTION__, message_id, message->getMessageTokenValue());
	}

	// TODO percy william felipe COMMS cudf0.12
	//auto concreteMessage = std::static_pointer_cast<PartitionPivotsMessage>(message);

	// WSM TODO waiting on message
	// return concreteMessage->getBlazingTable();
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
		//TODO WSM waiting on Factory::createColumnDataMessage
		// threads.push_back(std::thread([message_id, context_token, self_node, destination_node, columns]() mutable {
		// 	auto message = Factory::createColumnDataMessage(message_id, context_token, self_node, columns);
		// 	Client::send(destination_node, *message);
		// }));
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

		// TODO percy william felipe COMMS cudf0.12
		//auto column_message = std::static_pointer_cast<ColumnDataMessage>(message);
		
		// WSM TODO waiting on message Node refactor
		// auto node = message->getSenderNode();
		// int node_idx = context->getNodeIndex(node);
		// if(received[node_idx]) {
		// 	Library::Logging::Logger().logError(ral::utilities::buildLogString(std::to_string(context_token),
		// 		std::to_string(context->getQueryStep()),
		// 		std::to_string(context->getQuerySubstep()),
		// 		"ERROR: Already received collectSomePartitions from node " + std::to_string(node_idx)));
		// }
		// WSM waiting on getBlazingTable
		// node_columns.emplace_back(std::make_pair(node, std::move(column_message->getBlazingTable())));
		// received[node_idx] = true;
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

	std::unique_ptr<BlazingTable> groupedSamples; // replace this with commented below
	// WSM cudf0.12 waiting on groupby_without_aggregations
	// std::vector<int> groupColumnIndices(concatSamples.size());
	// std::iota(groupColumnIndices.begin(), groupColumnIndices.end(), 0);
	// std::unique_ptr<BlazingTable> groupedSamples =
	// 	ral::operators::groupby_without_aggregations(concatSamples, groupColumnIndices);

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