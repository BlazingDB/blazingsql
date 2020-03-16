#ifndef BLAZINGDB_RAL_DISTRIBUTION_PRIMITIVES_H
#define BLAZINGDB_RAL_DISTRIBUTION_PRIMITIVES_H

#include "blazingdb/manager/Context.h"
#include "communication/factory/MessageFactory.h"
#include <vector>
#include "execution_graph/logic_controllers/LogicPrimitives.h"


namespace ral {

namespace distribution {
namespace experimental {
	namespace {
		using Context = blazingdb::manager::experimental::Context;
		using Node = blazingdb::transport::experimental::Node;
	}  // namespace

	typedef std::pair<blazingdb::transport::experimental::Node, std::unique_ptr<ral::frame::BlazingTable> > NodeColumn;
	typedef std::pair<blazingdb::transport::experimental::Node, ral::frame::BlazingTableView > NodeColumnView;
	using namespace ral::frame;

	void sendSamplesToMaster(Context * context, const BlazingTableView & samples, std::size_t table_total_rows);
	std::pair<std::vector<NodeColumn>, std::vector<std::size_t> > collectSamples(Context * context);

	std::unique_ptr<BlazingTable> generatePartitionPlans(
				Context * context, std::vector<BlazingTableView> & samples, 
				const std::vector<std::size_t> & table_total_rows, const std::vector<int8_t> & sortOrderTypes);
	
	void distributePartitionPlan(Context * context, const BlazingTableView & pivots);

	std::unique_ptr<BlazingTable> getPartitionPlan(Context * context);
	
// This function locates the pivots in the table and partitions the data on those pivot points. 
// IMPORTANT: This function expects data to aready be sorted according to the searchColIndices and sortOrderTypes
// IMPORTANT: The TableViews of the data returned point to the same data that was input.
	std::vector<NodeColumnView> partitionData(Context * context,
											const BlazingTableView & table,
											const BlazingTableView & pivots,
											const std::vector<int> & searchColIndices,
											std::vector<int8_t> sortOrderTypes);


	void distributePartitions(Context * context, std::vector<NodeColumnView> & partitions);

	std::vector<NodeColumn> collectPartitions(Context * context);

	std::vector<NodeColumn> collectSomePartitions(Context * context, int num_partitions);

	void scatterData(Context * context, const BlazingTableView & table);

	std::unique_ptr<BlazingTable> sortedMerger(std::vector<BlazingTableView> & tables,
				const std::vector<int8_t> & sortOrderTypes, const std::vector<int> & sortColIndices);
	
	std::unique_ptr<BlazingTable> getPivotPointsTable(Context * context, const BlazingTableView & sortedSamples);

	std::unique_ptr<BlazingTable> generatePartitionPlansGroupBy(Context * context, std::vector<BlazingTableView> & samples);

	std::unique_ptr<BlazingTable> groupByWithoutAggregationsMerger(const std::vector<BlazingTableView> & tables, const std::vector<int> & group_column_indices);
	
	// multi-threaded message sender
	void broadcastMessage(std::vector<Node> nodes, 
			std::shared_ptr<communication::messages::experimental::Message> message);
			
	void distributeNumRows(Context * context, cudf::size_type num_rows);

	std::vector<cudf::size_type> collectNumRows(Context * context);	
	
	void distributeLeftRightNumRows(Context * context, std::size_t left_num_rows, std::size_t right_num_rows);

	void collectLeftRightNumRows(Context * context, std::vector<cudf::size_type> & node_num_rows_left,
				std::vector<cudf::size_type> & node_num_rows_right);

	void distributeLeftRightTableSizeBytes(Context * context, const ral::frame::BlazingTableView & left,
    		const ral::frame::BlazingTableView & right);

	void collectLeftRightTableSizeBytes(Context * context,	std::vector<int64_t> & node_num_bytes_left,
			std::vector<int64_t> & node_num_bytes_right);
	
}  // namespace experimental
}  // namespace distribution
}  // namespace ral

namespace ral {
namespace distribution {
namespace sampling {
namespace experimental {

std::unique_ptr<ral::frame::BlazingTable> generateSamplesFromRatio(
	const ral::frame::BlazingTableView & table, const double ratio);

std::unique_ptr<ral::frame::BlazingTable> generateSamples(
	const ral::frame::BlazingTableView & table, const size_t quantile);

}  // namespace experimental
}  // namespace sampling
}  // namespace distribution
}  // namespace ral

#endif  // BLAZINGDB_RAL_DISTRIBUTION_PRIMITIVES_H
