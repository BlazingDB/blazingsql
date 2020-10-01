#pragma once

#include "blazingdb/manager/Context.h"
#include "communication/factory/MessageFactory.h"
#include <vector>
#include "execution_graph/logic_controllers/LogicPrimitives.h"

namespace ral {
namespace distribution {

	namespace {
		using Context = blazingdb::manager::Context;
		using Node = blazingdb::transport::Node;
	}  // namespace

	typedef std::pair<blazingdb::transport::Node, std::unique_ptr<ral::frame::BlazingTable> > NodeColumn;
	typedef std::pair<blazingdb::transport::Node, ral::frame::BlazingTableView > NodeColumnView;
	using namespace ral::frame;

	void sendSamplesToMaster(Context * context, const BlazingTableView & samples, std::size_t table_total_rows);
	
	std::unique_ptr<BlazingTable> generatePartitionPlans(
				cudf::size_type number_partitions, const std::vector<BlazingTableView> & samples,
				const std::vector<cudf::order> & sortOrderTypes);

	void distributePartitionPlan(Context * context, const BlazingTableView & pivots);

	std::unique_ptr<BlazingTable> getPartitionPlan(Context * context);

// This function locates the pivots in the table and partitions the data on those pivot points.
// IMPORTANT: This function expects data to aready be sorted according to the searchColIndices and sortOrderTypes
// IMPORTANT: The TableViews of the data returned point to the same data that was input.
	std::vector<NodeColumnView> partitionData(Context * context,
											const BlazingTableView & table,
											const BlazingTableView & pivots,
											const std::vector<int> & searchColIndices,
											std::vector<cudf::order> sortOrderTypes);

	void distributeTablePartitions(Context * context, std::vector<NodeColumnView> & partitions, const std::vector<int32_t> & part_ids = std::vector<int32_t>());

	void notifyLastTablePartitions(Context * context, std::string message_id);

	void distributePartitions(Context * context, std::vector<NodeColumnView> & partitions);



	void scatterData(Context * context, const BlazingTableView & table);

	std::unique_ptr<BlazingTable> sortedMerger(std::vector<BlazingTableView> & tables,
				const std::vector<cudf::order> & sortOrderTypes, const std::vector<int> & sortColIndices);

	std::unique_ptr<BlazingTable> getPivotPointsTable(cudf::size_type number_pivots, const BlazingTableView & sortedSamples);

	// multi-threaded message sender
	void broadcastMessage(Context * context, std::vector<Node> nodes,
			std::shared_ptr<communication::messages::Message> message);

	void distributeNumRows(Context * context, int64_t num_rows);


	void distributeLeftRightTableSizeBytes(Context * context, int64_t bytes_left, int64_t bytes_right);

	void collectLeftRightTableSizeBytes(Context * context,	std::vector<int64_t> & node_num_bytes_left,
			std::vector<int64_t> & node_num_bytes_right);

}  // namespace distribution
}  // namespace ral
