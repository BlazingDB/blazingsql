#include "distribution/primitives.h"
#include "CalciteExpressionParsing.h"
#include "communication/CommunicationData.h"

#include <cmath>

#include <cudf/search.hpp>
#include <cudf/sorting.hpp>
#include "cudf/detail/gather.hpp"
#include "cudf/copying.hpp"
#include <cudf/merge.hpp>
#include <cudf/utilities/traits.hpp>

#include "utilities/CommonOperations.h"

#include "error.hpp"
#include "utilities/ctpl_stl.h"
#include <numeric>

#include <spdlog/spdlog.h>
using namespace fmt::literals;

namespace ral {
namespace distribution {

typedef ral::frame::BlazingTable BlazingTable;
typedef ral::frame::BlazingTableView BlazingTableView;
typedef blazingdb::manager::Context Context;
typedef blazingdb::transport::Node Node;



std::unique_ptr<BlazingTable> generatePartitionPlans(
				cudf::size_type number_partitions, const std::vector<BlazingTableView> & samples,
				const std::vector<cudf::order> & sortOrderTypes) {

	std::unique_ptr<BlazingTable> concatSamples = ral::utilities::concatTables(samples);
	

	std::vector<cudf::null_order> null_orders(sortOrderTypes.size(), cudf::null_order::AFTER);
	// TODO this is just a default setting. Will want to be able to properly set null_order
	std::unique_ptr<cudf::column> sort_indices = cudf::sorted_order( concatSamples->view(), sortOrderTypes, null_orders);

	std::unique_ptr<CudfTable> sortedSamples = cudf::detail::gather( concatSamples->view(), sort_indices->view(), cudf::detail::out_of_bounds_policy::FAIL, cudf::detail::negative_index_policy::NOT_ALLOWED  );

	// lets get names from a non-empty table
	std::vector<std::string> names;
	for(size_t i = 0; i < samples.size(); i++) {
		if (samples[i].names().size() > 0){
			names = samples[i].names();
			break;
		}
	}
	if(names.size() == 0){
		throw std::exception();
	}

	return getPivotPointsTable(number_partitions, BlazingTableView(sortedSamples->view(), names));
}


// This function locates the pivots in the table and partitions the data on those pivot points.
// IMPORTANT: This function expects data to aready be sorted according to the searchColIndices and sortOrderTypes
// IMPORTANT: The TableViews of the data returned point to the same data that was input.
std::vector<NodeColumnView> partitionData(Context * context,
											const BlazingTableView & table,
											const BlazingTableView & pivots,
											const std::vector<int> & searchColIndices,
											std::vector<cudf::order> sortOrderTypes) {

	RAL_EXPECTS(static_cast<size_t>(pivots.view().num_columns()) == searchColIndices.size(), "Mismatched pivots num_columns and searchColIndices");

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
	for (int i = 0; static_cast<size_t>(i) < partitioned_data.size(); i++){
		int node_idx = std::min(i / step, static_cast<int>(all_nodes.size() - 1));
		partitioned_node_column_views.emplace_back(all_nodes[node_idx], BlazingTableView(partitioned_data[i], table.names()));
	}

	return partitioned_node_column_views;
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

	std::unique_ptr<CudfTable> pivots = cudf::detail::gather( sortedSamples.view(), gather_map->view(), cudf::detail::out_of_bounds_policy::FAIL, cudf::detail::negative_index_policy::NOT_ALLOWED );

	return std::make_unique<BlazingTable>(std::move(pivots), sortedSamples.names());
}



}  // namespace distribution
}  // namespace ral
