#include "distribution/primitives.h"
#include "CalciteExpressionParsing.h"
#include "communication/CommunicationData.h"
#include "communication/network/Client.h"
#include "communication/network/Server.h"
#include "utilities/StringUtils.h"
#include <blazingdb/io/Library/Logging/Logger.h>
#include <cmath>

#include <cudf/search.hpp>
#include <cudf/sorting.hpp>
#include <cudf/merge.hpp>
#include <from_cudf/cpp_tests/utilities/column_wrapper.hpp>
#include <from_cudf/cpp_tests/utilities/column_utilities.hpp>

#include "utilities/CommonOperations.h"
#include "utilities/DebuggingUtils.h"
#include "utilities/random_generator.cuh"
#include "Utils.cuh"

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
typedef ral::communication::messages::experimental::ColumnDataPartitionMessage ColumnDataPartitionMessage;
typedef ral::communication::messages::experimental::ReceivedDeviceMessage ReceivedDeviceMessage;
typedef ral::communication::experimental::CommunicationData CommunicationData;
typedef ral::communication::network::experimental::Server Server;
typedef ral::communication::network::experimental::Client Client;



void sendSamplesToMaster(Context * context, const BlazingTableView & samples, std::size_t table_total_rows) {
  // Get master node
	const Node & master_node = context->getMasterNode();

	// Get self node
	Node self_node = CommunicationData::getInstance().getSelfNode();

	// Get context token
	std::string context_comm_token = context->getContextCommunicationToken();
	const uint32_t context_token = context->getContextToken();
	const std::string message_id = SampleToNodeMasterMessage::MessageID() + "_" + context_comm_token;

	auto message =
		Factory::createSampleToNodeMaster(message_id, context_token, self_node, table_total_rows, samples);

	// Send message to master
	Client::send(master_node, *message);
}

std::pair<std::vector<NodeColumn>, std::vector<std::size_t> > collectSamples(Context * context) {

	std::string context_comm_token = context->getContextCommunicationToken();
	const uint32_t context_token = context->getContextToken();
	const std::string message_id = SampleToNodeMasterMessage::MessageID() + "_" + context_comm_token;

	std::vector<NodeColumn> nodeColumns;
	std::vector<std::size_t> table_total_rows;

	size_t size = context->getWorkerNodes().size();
	std::vector<bool> received(context->getTotalNodes(), false);
	for(int k = 0; k < size; ++k) {
		auto message = Server::getInstance().getMessage(context_token, message_id);

		auto node = message->getSenderNode();
		int node_idx = context->getNodeIndex(node);
		if(received[node_idx]) {
			Library::Logging::Logger().logError(ral::utilities::buildLogString(std::to_string(context_token),
				std::to_string(context->getQueryStep()),
				std::to_string(context->getQuerySubstep()),
				"ERROR: Already received collectSamples from node " + std::to_string(node_idx)));
		}
		auto concreteMessage = std::static_pointer_cast<ReceivedDeviceMessage>(message);
		table_total_rows.push_back(concreteMessage->getTotalRowSize());
		nodeColumns.emplace_back(std::make_pair(node, std::move(concreteMessage->releaseBlazingTable())));
		received[node_idx] = true;
	}

	return std::make_pair(std::move(nodeColumns), table_total_rows);
}


std::unique_ptr<BlazingTable> generatePartitionPlans(
				cudf::size_type number_partitions, const std::vector<BlazingTableView> & samples, 
				const std::vector<std::size_t> & table_total_rows, const std::vector<cudf::order> & sortOrderTypes) {

	std::unique_ptr<BlazingTable> concatSamples = ral::utilities::experimental::concatTables(samples);

	std::vector<cudf::null_order> null_orders(sortOrderTypes.size(), cudf::null_order::AFTER);
	// TODO this is just a default setting. Will want to be able to properly set null_order
	std::unique_ptr<cudf::column> sort_indices = cudf::experimental::sorted_order( concatSamples->view(), sortOrderTypes, null_orders);

	std::unique_ptr<CudfTable> sortedSamples = cudf::experimental::gather( concatSamples->view(), sort_indices->view() );

	// lets get names from a non-empty table
	std::vector<std::string> names;
	for(size_t i = 0; i < samples.size(); i++) {
		if (samples[i].names().size() > 0){
			names = samples[i].names();
			break;
		}
	}

	return getPivotPointsTable(number_partitions, BlazingTableView(sortedSamples->view(), names));
}

void distributePartitionPlan(Context * context, const BlazingTableView & pivots) {

	std::string context_comm_token = context->getContextCommunicationToken();
	const uint32_t context_token = context->getContextToken();
	const std::string message_id = PartitionPivotsMessage::MessageID() + "_" + context_comm_token;

	auto node = CommunicationData::getInstance().getSelfNode();
	auto message = Factory::createPartitionPivotsMessage(message_id, context_token, node, pivots);
	broadcastMessage(context->getWorkerNodes(), message);
}

std::unique_ptr<BlazingTable> getPartitionPlan(Context * context) {

	std::string context_comm_token = context->getContextCommunicationToken();
	const uint32_t context_token = context->getContextToken();
	const std::string message_id = PartitionPivotsMessage::MessageID() + "_" + context_comm_token;

	auto message = Server::getInstance().getMessage(context_token, message_id);

	auto concreteMessage = std::static_pointer_cast<ReceivedDeviceMessage>(message);
	return concreteMessage->releaseBlazingTable();
}


// This function locates the pivots in the table and partitions the data on those pivot points.
// IMPORTANT: This function expects data to aready be sorted according to the searchColIndices and sortOrderTypes
// IMPORTANT: The TableViews of the data returned point to the same data that was input.
std::vector<NodeColumnView> partitionData(Context * context,
											const BlazingTableView & table,
											const BlazingTableView & pivots,
											const std::vector<int> & searchColIndices,
											std::vector<cudf::order> sortOrderTypes) {

	RAL_EXPECTS(pivots.view().num_columns() == searchColIndices.size(), "Mismatched pivots num_columns and searchColIndices");

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
		sortOrderTypes.assign(searchColIndices.size(), cudf::order::ASCENDING);
	}

	// TODO this is just a default setting. Will want to be able to properly set null_order
	std::vector<cudf::null_order> null_orders(sortOrderTypes.size(), cudf::null_order::AFTER);

	CudfTableView columns_to_search = table.view().select(searchColIndices);

	std::unique_ptr<cudf::column> pivot_indexes = cudf::experimental::upper_bound(columns_to_search,
                                    pivots.view(),
                                    sortOrderTypes,
                                    null_orders);

	std::vector<cudf::size_type> host_data(pivot_indexes->view().size());
	CUDA_TRY(cudaMemcpy(host_data.data(), pivot_indexes->view().data<cudf::size_type>(), pivot_indexes->view().size() * sizeof(cudf::size_type), cudaMemcpyDeviceToHost));
	
	std::vector<CudfTableView> partitioned_data = cudf::experimental::split(table.view(), host_data);

	std::vector<Node> all_nodes = context->getAllNodes();

	RAL_EXPECTS(all_nodes.size() <= partitioned_data.size(), "Number of table partitions is smalled than total nodes");

	int step = static_cast<int>(partitioned_data.size() / all_nodes.size());
	std::vector<NodeColumnView> partitioned_node_column_views;
	for (int i = 0; i < partitioned_data.size(); i++){
		int node_idx = std::min(i / step, static_cast<int>(all_nodes.size() - 1));
		partitioned_node_column_views.push_back(std::make_pair(all_nodes[node_idx], BlazingTableView(partitioned_data[i], table.names())));
	}
	
	return partitioned_node_column_views;
}

void distributeTablePartitions(Context * context, std::vector<NodeColumnView> & partitions) {

	std::string context_comm_token = context->getContextCommunicationToken();
	const uint32_t context_token = context->getContextToken();
	const std::string message_id = ColumnDataPartitionMessage::MessageID() + "_" + context_comm_token;

	auto self_node = CommunicationData::getInstance().getSelfNode();
	std::vector<BlazingThread> threads;
	for (auto i = 0; i < partitions.size(); i++){
		auto & nodeColumn = partitions[i];
		if(nodeColumn.first == self_node) {
			continue;
		}
		BlazingTableView columns = nodeColumn.second;
		auto destination_node = nodeColumn.first;
		int partition_id = static_cast<int>(i);
		threads.push_back(BlazingThread([message_id, context_token, self_node, destination_node, columns, partition_id]() mutable {
			auto message = Factory::createColumnDataPartitionMessage(message_id, context_token, self_node, partition_id, columns);
			Client::send(destination_node, *message);
		}));
	}
	for(size_t i = 0; i < threads.size(); i++) {
		threads[i].join();
	}
}

void notifyLastTablePartitions(Context * context, std::string message_id) {
	std::string context_comm_token = context->getContextCommunicationToken();
	const uint32_t context_token = context->getContextToken();
	const std::string full_message_id = message_id + "_" + context_comm_token;

	auto self_node = CommunicationData::getInstance().getSelfNode();
	auto nodes = context->getAllNodes();
	for(std::size_t i = 0; i < nodes.size(); ++i) {
		if(!(nodes[i] == self_node)) {
			blazingdb::transport::experimental::Message::MetaData metadata;
			std::strcpy(metadata.messageToken, full_message_id.c_str());
			metadata.contextToken = context_token;
			Client::notifyLastMessageEvent(nodes[i], metadata);
		}
	}
}

void distributePartitions(Context * context, std::vector<NodeColumnView> & partitions) {

	std::string context_comm_token = context->getContextCommunicationToken();
	const uint32_t context_token = context->getContextToken();
	const std::string message_id = ColumnDataMessage::MessageID() + "_" + context_comm_token;

	auto self_node = CommunicationData::getInstance().getSelfNode();
	std::vector<BlazingThread> threads;
	for(auto & nodeColumn : partitions) {
		if(nodeColumn.first == self_node) {
			continue;
		}
		BlazingTableView columns = nodeColumn.second;
		auto destination_node = nodeColumn.first;
		threads.push_back(BlazingThread([message_id, context_token, self_node, destination_node, columns]() mutable {
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
	std::string context_comm_token = context->getContextCommunicationToken();
	const uint32_t context_token = context->getContextToken();
	const std::string message_id = ColumnDataMessage::MessageID() + "_" + context_comm_token;

	while(0 < num_partitions) {
		auto message = Server::getInstance().getMessage(context_token, message_id);
		num_partitions--;

		auto node = message->getSenderNode();
		int node_idx = context->getNodeIndex(node);
		if(received[node_idx]) {
			Library::Logging::Logger().logError(ral::utilities::buildLogString(std::to_string(context_token),
				std::to_string(context->getQueryStep()),
				std::to_string(context->getQuerySubstep()),
				"ERROR: Already received collectSomePartitions from node " + std::to_string(node_idx)));
		}
		auto concreteMessage = std::static_pointer_cast<ReceivedDeviceMessage>(message);
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
	const std::vector<cudf::order> & sortOrderTypes,
	const std::vector<int> & sortColIndices) {

	// TODO this is just a default setting. Will want to be able to properly set null_order
	std::vector<cudf::null_order> null_orders(sortOrderTypes.size(), cudf::null_order::AFTER);

	std::unique_ptr<CudfTable> merged_table;
	CudfTableView left_table = tables[0].view();
	
	for(size_t i = 1; i < tables.size(); i++) {
		CudfTableView right_table = tables[i].view();
		merged_table = cudf::experimental::merge({left_table, right_table}, sortColIndices, sortOrderTypes, null_orders);
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


std::unique_ptr<BlazingTable> getPivotPointsTable(cudf::size_type number_partitions, const BlazingTableView & sortedSamples){

	cudf::size_type outputRowSize = sortedSamples.view().num_rows();
	cudf::size_type pivotsSize = outputRowSize > 0 ? number_partitions - 1 : 0;

	int32_t step = outputRowSize / number_partitions;

	auto sequence_iter = cudf::test::make_counting_transform_iterator(0, [step](auto i) { return int32_t(i * step) + step;});
	cudf::test::fixed_width_column_wrapper<int32_t> gather_map_wrapper(sequence_iter, sequence_iter + pivotsSize);
	CudfColumnView gather_map(gather_map_wrapper);
	std::unique_ptr<CudfTable> pivots = cudf::experimental::gather( sortedSamples.view(), gather_map );

	return std::make_unique<BlazingTable>(std::move(pivots), sortedSamples.names());
}


std::unique_ptr<BlazingTable> generatePartitionPlansGroupBy(Context * context, std::vector<BlazingTableView> & samples) {

	std::unique_ptr<BlazingTable> concatSamples = ral::utilities::experimental::concatTables(samples);
	
	std::vector<int> groupColumnIndices(concatSamples->num_columns());
	std::iota(groupColumnIndices.begin(), groupColumnIndices.end(), 0);
	std::unique_ptr<BlazingTable> groupedSamples = ral::operators::experimental::compute_groupby_without_aggregations(
														concatSamples->toBlazingTableView(), groupColumnIndices);
	
	// Sort
	std::vector<cudf::order> column_order(groupedSamples->num_columns(), cudf::order::ASCENDING);
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

	return getPivotPointsTable(context->getTotalNodes(), BlazingTableView(sortedSamples->view(), names));
}

std::unique_ptr<BlazingTable> groupByWithoutAggregationsMerger(
	const std::vector<BlazingTableView> & tables, const std::vector<int> & group_column_indices) {
	
	std::unique_ptr<BlazingTable> concatGroups = ral::utilities::experimental::concatTables(tables);

	return ral::operators::experimental::compute_groupby_without_aggregations(concatGroups->toBlazingTableView(),  group_column_indices);	
}

void broadcastMessage(std::vector<Node> nodes, 
			std::shared_ptr<communication::messages::experimental::Message> message) {
	std::vector<BlazingThread> threads(nodes.size());
	for(size_t i = 0; i < nodes.size(); i++) {
		Node node = nodes[i];
		threads[i] = BlazingThread([node, message]() {
			Client::send(node, *message);
		});
	}
	for(size_t i = 0; i < threads.size(); i++) {
		threads[i].join();
	}
}

void distributeNumRows(Context * context, int64_t num_rows) {
	
	std::string context_comm_token = context->getContextCommunicationToken();
	const uint32_t context_token = context->getContextToken();
	const std::string message_id = SampleToNodeMasterMessage::MessageID() + "_" + context_comm_token;

	auto self_node = CommunicationData::getInstance().getSelfNode();
	auto message = Factory::createSampleToNodeMaster(message_id, context_token, self_node, num_rows, {});

	int self_node_idx = context->getNodeIndex(CommunicationData::getInstance().getSelfNode());
	broadcastMessage(context->getAllOtherNodes(self_node_idx), message);
}

std::vector<int64_t> collectNumRows(Context * context) {
	
	int num_nodes = context->getTotalNodes();
	std::vector<int64_t> node_num_rows(num_nodes);
	std::vector<bool> received(num_nodes, false);

	std::string context_comm_token = context->getContextCommunicationToken();
	const uint32_t context_token = context->getContextToken();
	const std::string message_id = SampleToNodeMasterMessage::MessageID() + "_" + context_comm_token;

	int self_node_idx = context->getNodeIndex(CommunicationData::getInstance().getSelfNode());
	for(cudf::size_type i = 0; i < num_nodes - 1; ++i) {
		auto message = Server::getInstance().getMessage(context_token, message_id);
		auto concrete_message = std::static_pointer_cast<ReceivedDeviceMessage>(message);
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
	
	std::string context_comm_token = context->getContextCommunicationToken();
	const uint32_t context_token = context->getContextToken();
	const std::string message_id = SampleToNodeMasterMessage::MessageID() + "_" + context_comm_token;

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

	std::string context_comm_token = context->getContextCommunicationToken();
	const uint32_t context_token = context->getContextToken();
	const std::string message_id = SampleToNodeMasterMessage::MessageID() + "_" + context_comm_token;

	int self_node_idx = context->getNodeIndex(CommunicationData::getInstance().getSelfNode());
	for(cudf::size_type i = 0; i < num_nodes - 1; ++i) {
		auto message = Server::getInstance().getMessage(context_token, message_id);
		auto concrete_message = std::static_pointer_cast<ReceivedDeviceMessage>(message);
		auto node = concrete_message->getSenderNode();
		std::unique_ptr<BlazingTable> num_rows_data = concrete_message->releaseBlazingTable();
		assert(num_rows_data->view().num_columns() == 1);
		assert(num_rows_data->view().num_rows() == 2);
		
		std::vector<cudf::size_type> host_data(num_rows_data->view().column(0).size());
		CUDA_TRY(cudaMemcpy(host_data.data(), num_rows_data->view().column(0).data<cudf::size_type>(), num_rows_data->view().column(0).size() * sizeof(cudf::size_type), cudaMemcpyDeviceToHost));
		
		int node_idx = context->getNodeIndex(node);
		assert(node_idx >= 0);
		if(received[node_idx]) {
			Library::Logging::Logger().logError(ral::utilities::buildLogString(std::to_string(context_token),
				std::to_string(context->getQueryStep()),
				std::to_string(context->getQuerySubstep()),
				"ERROR: Already received collectLeftRightNumRows from node " + std::to_string(node_idx)));
		}
		node_num_rows_left[node_idx] = host_data[0];
		node_num_rows_right[node_idx] = host_data[1];
		received[node_idx] = true;
	}
}

void distributeLeftRightTableSizeBytes(Context * context, int64_t bytes_left, int64_t bytes_right) {

	const std::string context_comm_token = context->getContextCommunicationToken();
	const uint32_t context_token = context->getContextToken();
	const std::string message_id = SampleToNodeMasterMessage::MessageID() + "_" + context_comm_token;

	auto self_node = CommunicationData::getInstance().getSelfNode();
	cudf::test::fixed_width_column_wrapper<int64_t>num_bytes_col{bytes_left, bytes_right};
	CudfTableView num_bytes_table{{num_bytes_col}};
	std::vector<std::string> names{"left_num_bytes", "right_num_bytes"};
	BlazingTableView num_bytes_blz_table(num_bytes_table, names);
	auto message = Factory::createSampleToNodeMaster(message_id, context_token, self_node, 0, num_bytes_blz_table);

	int self_node_idx = context->getNodeIndex(CommunicationData::getInstance().getSelfNode());
	broadcastMessage(context->getAllOtherNodes(self_node_idx), message);
}

void collectLeftRightTableSizeBytes(Context * context,	std::vector<int64_t> & node_num_bytes_left,
			std::vector<int64_t> & node_num_bytes_right) {
	
	int num_nodes = context->getTotalNodes();
	node_num_bytes_left.resize(num_nodes);
	node_num_bytes_right.resize(num_nodes);
	std::vector<bool> received(num_nodes, false);

	const std::string context_comm_token = context->getContextCommunicationToken();
	const uint32_t context_token = context->getContextToken();
	const std::string message_id = SampleToNodeMasterMessage::MessageID() + "_" + context_comm_token;

	int self_node_idx = context->getNodeIndex(CommunicationData::getInstance().getSelfNode());
	for(cudf::size_type i = 0; i < num_nodes - 1; ++i) {
		auto message = Server::getInstance().getMessage(context_token, message_id);
		auto concrete_message = std::static_pointer_cast<ReceivedDeviceMessage>(message);
		auto node = concrete_message->getSenderNode();
		std::unique_ptr<BlazingTable> num_bytes_data = concrete_message->releaseBlazingTable();
		assert(num_bytes_data->view().num_columns() == 1);
		assert(num_bytes_data->view().num_rows() == 2);
		
		std::vector<int64_t> host_data(num_bytes_data->view().column(0).size());
		CUDA_TRY(cudaMemcpy(host_data.data(), num_bytes_data->view().column(0).data<int64_t>(), num_bytes_data->view().column(0).size() * sizeof(int64_t), cudaMemcpyDeviceToHost));
		
		int node_idx = context->getNodeIndex(node);
		assert(node_idx >= 0);
		if(received[node_idx]) {
			Library::Logging::Logger().logError(ral::utilities::buildLogString(std::to_string(context_token),
				std::to_string(context->getQueryStep()),
				std::to_string(context->getQuerySubstep()),
				"ERROR: Already received collectLeftRightTableSizeBytes from node " + std::to_string(node_idx)));
		}
		node_num_bytes_left[node_idx] = host_data[0];
		node_num_bytes_right[node_idx] = host_data[1];
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

std::unique_ptr<ral::frame::BlazingTable> generateSamplesFromRatio(
	const ral::frame::BlazingTableView & table, const double ratio) {
	return generateSamples(table, std::ceil(table.view().num_rows() * ratio));
}

std::unique_ptr<ral::frame::BlazingTable> generateSamples(
	const ral::frame::BlazingTableView & table, const size_t quantile) {

	return ral::generator::generate_sample(table, quantile);
}

}  // namespace experimental
}  // namespace sampling
}  // namespace distribution
}  // namespace ral
