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

	
	std::unique_ptr<BlazingTable> generatePartitionPlans(
				cudf::size_type number_partitions, const std::vector<BlazingTableView> & samples,
				const std::vector<cudf::order> & sortOrderTypes);

	
	std::unique_ptr<BlazingTable> getPartitionPlan(Context * context);

// This function locates the pivots in the table and partitions the data on those pivot points.
// IMPORTANT: This function expects data to aready be sorted according to the searchColIndices and sortOrderTypes
// IMPORTANT: The TableViews of the data returned point to the same data that was input.
	std::vector<NodeColumnView> partitionData(Context * context,
											const BlazingTableView & table,
											const BlazingTableView & pivots,
											const std::vector<int> & searchColIndices,
											std::vector<cudf::order> sortOrderTypes);

	


	
	
	std::unique_ptr<BlazingTable> getPivotPointsTable(cudf::size_type number_pivots, const BlazingTableView & sortedSamples);

	
	std::unique_ptr<BlazingTable> sortedMerger(std::vector<BlazingTableView> & tables,
				const std::vector<cudf::order> & sortOrderTypes, const std::vector<int> & sortColIndices);
}  // namespace distribution
}  // namespace ral
