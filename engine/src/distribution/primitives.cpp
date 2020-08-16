#include "distribution/primitives.h"
#include "CalciteExpressionParsing.h"
#include "communication/CommunicationData.h"
#include "communication/network/Client.h"
#include "communication/network/Server.h"
#include "utilities/StringUtils.h"
#include <cmath>

#include <cudf/search.hpp>
#include <cudf/sorting.hpp>
#include <cudf/merge.hpp>
#include <cudf/utilities/traits.hpp>

#include "utilities/CommonOperations.h"
#include "error.hpp"

#include <spdlog/spdlog.h>
using namespace fmt::literals;

namespace ral {
namespace distribution {

typedef ral::frame::BlazingTable BlazingTable;
typedef ral::frame::BlazingTableView BlazingTableView;
typedef blazingdb::manager::Context Context;
typedef blazingdb::transport::Node Node;
typedef ral::communication::messages::Factory Factory;
typedef ral::communication::messages::SampleToNodeMasterMessage SampleToNodeMasterMessage;
typedef ral::communication::messages::PartitionPivotsMessage PartitionPivotsMessage;
typedef ral::communication::messages::ColumnDataMessage ColumnDataMessage;
typedef ral::communication::messages::ColumnDataPartitionMessage ColumnDataPartitionMessage;
typedef ral::communication::messages::ReceivedDeviceMessage ReceivedDeviceMessage;
typedef ral::communication::CommunicationData CommunicationData;
typedef ral::communication::network::Server Server;
typedef ral::communication::network::Client Client;

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
	auto logger = spdlog::get("batch_logger");
	for(int k = 0; k < size; ++k) {
		auto message = Server::getInstance().getMessage(context_token, message_id);
		logger->warn("|||{info}|{duration}||||",
				"info"_a="Server::getInstance().getMessage() finished ok - from primitives.cpp->collectSamples() - worker node: " + std::to_string(k),
				"duration"_a="");

		auto node = message->getSenderNode();
		int node_idx = context->getNodeIndex(node);
		if(received[node_idx]) {
			auto logger = spdlog::get("batch_logger");
			logger->error("{query_id}|{step}|{substep}|{info}|{duration}||||",
							"query_id"_a=context->getContextToken(),
							"step"_a=context->getQueryStep(),
							"substep"_a=context->getQuerySubstep(),
							"info"_a="Already received collectSamples from node " + std::to_string(node_idx),
							"duration"_a="");
		}
		auto concreteMessage = std::static_pointer_cast<ReceivedDeviceMessage>(message);
		table_total_rows.push_back(concreteMessage->getTotalRowSize());
		nodeColumns.emplace_back(std::make_pair(node, std::move(concreteMessage->releaseBlazingTable())));
		received[node_idx] = true;
	}

	return std::make_pair(std::move(nodeColumns), table_total_rows);
}


std::unique_ptr<BlazingTable> generatePartitionPlans(
	cudf::size_type number_partitions,
	const std::vector<BlazingTableView> & samples,
	const std::vector<cudf::order> & sortOrderTypes,
	Context * context) {

	std::unique_ptr<BlazingTable> concatSamples = ral::utilities::concatTables(samples);
	
	//  setting just for experiments on RAPLAB
	//const uint32_t context_token = context->getContextToken();
	//std::string context_comm_token = context->getContextCommunicationToken();
	//const std::string message_id = ColumnDataMessage::MessageID() + "_" + context_comm_token;
	//auto message = Server::getInstance().getMessage(context_token, message_id); /// WHEN ADD THIS LINE ... HANG 
	auto logger = spdlog::get("batch_logger");
	//int node_idx = context->getNodeIndex(node);
	logger->warn("|||{info}|{duration}||||",
				"info"_a="concatTables() finished ok - from primitives.cpp->generatePartitionPlans()",
				"duration"_a="");

	std::vector<cudf::null_order> null_orders(sortOrderTypes.size(), cudf::null_order::AFTER);

	logger->warn("|||{info}|{duration}||||",
				"info"_a="null_orders() finished ok - from primitives.cpp->generatePartitionPlans()",
				"duration"_a="");

	// TODO this is just a default setting. Will want to be able to properly set null_order
	std::unique_ptr<cudf::column> sort_indices = cudf::sorted_order( concatSamples->view(), sortOrderTypes, null_orders);

	logger->warn("|||{info}|{duration}||||",
				"info"_a="cudf::sorted_order() finished ok - from primitives.cpp->generatePartitionPlans() ",
				"duration"_a="");

	std::unique_ptr<CudfTable> sortedSamples = cudf::gather( concatSamples->view(), sort_indices->view() );

	logger->warn("|||{info}|{duration}||||",
				"info"_a="cudf::gather() finished ok - from primitives.cpp->generatePartitionPlans() ",
				"duration"_a="");

	// lets get names from a non-empty table
	std::vector<std::string> names;
	for(size_t i = 0; i < samples.size(); i++) {
		if (samples[i].names().size() > 0){
			names = samples[i].names();
			break;
		}
	}
	std::unique_ptr<BlazingTable> output = getPivotPointsTable(number_partitions, BlazingTableView(sortedSamples->view(), names));

	logger->warn("|||{info}|{duration}||||",
				"info"_a="getPivotPointsTable() finished ok - from primitives.cpp->generatePartitionPlans()",
				"duration"_a="");

	return output;
}

void distributePartitionPlan(Context * context, const BlazingTableView & pivots) {

	std::string context_comm_token = context->getContextCommunicationToken();
	const uint32_t context_token = context->getContextToken();
	const std::string message_id = PartitionPivotsMessage::MessageID() + "_" + context_comm_token;
	auto logger = spdlog::get("batch_logger");
	logger->warn("|||{info}|{duration}||||",
				"info"_a="getContextCommunicationToken() finished ok - from primitives.cpp->distributePartitionPlan() ",
				"duration"_a="");

	auto node = CommunicationData::getInstance().getSelfNode();
	auto message = Factory::createPartitionPivotsMessage(message_id, context_token, node, pivots);
	broadcastMessage(context->getWorkerNodes(), message);
}

std::unique_ptr<BlazingTable> getPartitionPlan(Context * context) {

	std::string context_comm_token = context->getContextCommunicationToken();
	const uint32_t context_token = context->getContextToken();
	auto logger = spdlog::get("batch_logger");
	logger->warn("|||{info}|{duration}||||",
				"info"_a="getContextCommunicationToken() finished ok - from primitives.cpp->getPartitionPlan() ",
				"duration"_a="");

	const std::string message_id = PartitionPivotsMessage::MessageID() + "_" + context_comm_token;

	auto message = Server::getInstance().getMessage(context_token, message_id);
	logger->warn("|||{info}|{duration}||||",
				"info"_a="Server::getInstance().getMessage() finished ok - from primitives.cpp->getPartitionPlan()",
				"duration"_a="");

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

	std::unique_ptr<cudf::column> pivot_indexes = cudf::upper_bound(columns_to_search,
                                    pivots.view(),
                                    sortOrderTypes,
                                    null_orders);

	std::vector<cudf::size_type> host_data(pivot_indexes->view().size());
	CUDA_TRY(cudaMemcpy(host_data.data(), pivot_indexes->view().data<cudf::size_type>(), pivot_indexes->view().size() * sizeof(cudf::size_type), cudaMemcpyDeviceToHost));

	std::vector<CudfTableView> partitioned_data = cudf::split(table.view(), host_data);

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

void distributeTablePartitions(Context * context, std::vector<NodeColumnView> & partitions, const std::vector<int32_t> & part_ids) {

	std::string context_comm_token = context->getContextCommunicationToken();
	const uint32_t context_token = context->getContextToken();
	const std::string message_id = ColumnDataPartitionMessage::MessageID() + "_" + context_comm_token;

	auto self_node = CommunicationData::getInstance().getSelfNode();
	ctpl::thread_pool<BlazingThread> threads(20);  // ADD here pool thread as the other cases
	for (auto i = 0; i < partitions.size(); i++){
		auto & nodeColumn = partitions[i];
		if(nodeColumn.first == self_node) {
			continue;
		}
		// we dont want to send empty tables
		if (nodeColumn.second.num_rows() > 0){
			BlazingTableView columns = nodeColumn.second;
			auto destination_node = nodeColumn.first;
			int partition_id = part_ids.size() > i ? part_ids[i] : 0; // if part_ids is not set, then it does not matter and we can just use 0 as the partition_id

			threads.push(([message_id, context_token, self_node, destination_node, columns, partition_id](int thread_id) mutable {
				auto message = Factory::createColumnDataPartitionMessage(message_id, context_token, self_node, partition_id, columns);
				Client::send(destination_node, *message);
			}));
		}
	}
	//for(size_t i = 0; i < threads.size(); i++) {
	//	threads[i].join();
	//}
}

void notifyLastTablePartitions(Context * context, std::string message_id) {
	std::string context_comm_token = context->getContextCommunicationToken();
	const uint32_t context_token = context->getContextToken();
	const std::string full_message_id = message_id + "_" + context_comm_token;

	auto self_node = CommunicationData::getInstance().getSelfNode();
	auto nodes = context->getAllNodes();
	for(std::size_t i = 0; i < nodes.size(); ++i) {
		if(!(nodes[i] == self_node)) {
			blazingdb::transport::Message::MetaData metadata;
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
	ctpl::thread_pool<BlazingThread> threads(20); //  setting just for experiments on RAPLAB
	for(auto & nodeColumn : partitions) {
		if(nodeColumn.first == self_node) {
			continue;
		}
		BlazingTableView columns = nodeColumn.second;
		auto destination_node = nodeColumn.first;
		threads.push([message_id, context_token, self_node, destination_node, columns](int thread_id) mutable {
			auto message = Factory::createColumnDataMessage(message_id, context_token, self_node, columns);
			Client::send(destination_node, *message);
		});
	}
	//or(size_t i = 0; i < threads.size(); i++) {
	//	threads[i].join();
	//}
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
	auto logger = spdlog::get("batch_logger");
	while(0 < num_partitions) {
		auto message = Server::getInstance().getMessage(context_token, message_id);
		logger->warn("|||{info}|{duration}||||",
				"info"_a="Server::getInstance().getMessage() finished ok - from primitives.cpp->collectSomePartitions() - num_partitions " + std::to_string(num_partitions),
				"duration"_a="");
		num_partitions--;

		auto node = message->getSenderNode();
		int node_idx = context->getNodeIndex(node);
		if(received[node_idx]) {
			auto logger = spdlog::get("batch_logger");
			logger->error("{query_id}|{step}|{substep}|{info}|{duration}||||",
							"query_id"_a=context->getContextToken(),
							"step"_a=context->getQueryStep(),
							"substep"_a=context->getQuerySubstep(),
							"info"_a="Already received collectSomePartitions from node " + std::to_string(node_idx),
							"duration"_a="");
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

	std::vector<CudfTableView> cudf_table_views(tables.size());
	for(size_t i = 0; i < tables.size(); i++) {
		cudf_table_views[i] = tables[i].view();
	}
	std::unique_ptr<CudfTable> merged_table = cudf::merge(cudf_table_views, sortColIndices, sortOrderTypes, null_orders);

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

	std::vector<int32_t> sequence(pivotsSize);
    std::iota(sequence.begin(), sequence.end(), 1);
    std::transform(sequence.begin(), sequence.end(), sequence.begin(), [step](int32_t i){ return i*step;});

	auto gather_map = ral::utilities::vector_to_column(sequence, cudf::data_type(cudf::type_id::INT32));

	std::unique_ptr<CudfTable> pivots = cudf::gather( sortedSamples.view(), gather_map->view() );

	return std::make_unique<BlazingTable>(std::move(pivots), sortedSamples.names());
}

void broadcastMessage(std::vector<Node> nodes,
			std::shared_ptr<communication::messages::Message> message) {
	ctpl::thread_pool<BlazingThread> threads(20); //  setting just for experiments on RAPLAB
	for(size_t i = 0; i < nodes.size(); i++) {
		Node node = nodes[i];
		threads.push([node, message](int thread_id) {
			Client::send(node, *message);
		});
	}
	//for(size_t i = 0; i < threads.size(); i++) {
	//	threads[i].join();
	//}
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
	auto logger = spdlog::get("batch_logger");
	for(cudf::size_type i = 0; i < num_nodes - 1; ++i) {
		auto message = Server::getInstance().getMessage(context_token, message_id);
		logger->warn("|||{info}|{duration}||||",
				"info"_a="Server::getInstance().getMessage() finished ok - from primitives.cpp->collectNumRows() - worker node: " + std::to_string(i),
				"duration"_a="");

		auto concrete_message = std::static_pointer_cast<ReceivedDeviceMessage>(message);
		auto node = concrete_message->getSenderNode();
		int node_idx = context->getNodeIndex(node);
		assert(node_idx >= 0);
		if(received[node_idx]) {
			auto logger = spdlog::get("batch_logger");
			logger->error("{query_id}|{step}|{substep}|{info}|{duration}||||",
							"query_id"_a=context->getContextToken(),
							"step"_a=context->getQueryStep(),
							"substep"_a=context->getQuerySubstep(),
							"info"_a="Already received collectNumRows from node " + std::to_string(node_idx),
							"duration"_a="");
		}
		node_num_rows[node_idx] = concrete_message->getTotalRowSize();
		received[node_idx] = true;
	}

	return node_num_rows;
}

void distributeLeftRightTableSizeBytes(Context * context, int64_t bytes_left, int64_t bytes_right) {

	const std::string context_comm_token = context->getContextCommunicationToken();
	const uint32_t context_token = context->getContextToken();
	const std::string message_id = SampleToNodeMasterMessage::MessageID() + "_" + context_comm_token;

	auto self_node = CommunicationData::getInstance().getSelfNode();
	std::vector<int64_t> num_bytes_col_vect{bytes_left, bytes_right};
	auto num_bytes_col = ral::utilities::vector_to_column(num_bytes_col_vect, cudf::data_type(cudf::type_id::INT64));

	CudfTableView num_bytes_table{{num_bytes_col->view()}};
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
	auto logger = spdlog::get("batch_logger");
	for(cudf::size_type i = 0; i < num_nodes - 1; ++i) {
		auto message = Server::getInstance().getMessage(context_token, message_id);
		logger->warn("|||{info}|{duration}||||",
				"info"_a="Server::getInstance().getMessage() finished ok - from primitives.cpp->collectLeftRightTableSizeBytes() - worker node: " + std::to_string(i),
				"duration"_a="");

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
			auto logger = spdlog::get("batch_logger");
			logger->error("{query_id}|{step}|{substep}|{info}|{duration}||||",
							"query_id"_a=context->getContextToken(),
							"step"_a=context->getQueryStep(),
							"substep"_a=context->getQuerySubstep(),
							"info"_a="Already received collectLeftRightTableSizeBytes from node " + std::to_string(node_idx),
							"duration"_a="");
		}
		node_num_bytes_left[node_idx] = host_data[0];
		node_num_bytes_right[node_idx] = host_data[1];
		received[node_idx] = true;
	}
}

}  // namespace distribution
}  // namespace ral
