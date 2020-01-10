#ifndef BLAZINGDB_RAL_GROUPBY_OPERATOR_H
#define BLAZINGDB_RAL_GROUPBY_OPERATOR_H

#include "DataFrame.h"
#include <blazingdb/manager/Context.h>
#include <string>
#include <vector>

#include "execution_graph/logic_controllers/LogicPrimitives.h"
#include <cudf/aggregation.hpp>

#include <cudf/column/column_view.hpp>

namespace ral {
namespace operators {



namespace {
using blazingdb::manager::Context;
}  // namespace

bool is_aggregate(std::string query_part);

void process_aggregate(blazing_frame & input, std::string query_part, Context * queryContext);

std::vector<gdf_column_cpp> groupby_without_aggregations(
	std::vector<gdf_column_cpp> & input, const std::vector<int> & group_column_indices);

// TODO percy cudf0.12 create a detail/internal ns 4 these functions
void _new_aggregations_with_groupby(std::vector<CudfColumnView> const & group_by_columns,
	std::vector<cudf::column_view> const & aggregation_inputs,
	std::vector<std::unique_ptr<cudf::experimental::aggregation>> const & agg_ops,
	std::vector<cudf::mutable_column_view> & group_by_output_columns,
	std::vector<cudf::mutable_column_view> & aggregation_output_columns,
	std::vector<std::string> & output_column_names);

// TODO Rommel
std::unique_ptr<ral::frame::BlazingTable> _new_groupby_without_aggregations(
	const ral::frame::BlazingTableView & table, const std::vector<int> & group_column_indices);

}  // namespace operators
}  // namespace ral

#endif  // BLAZINGDB_RAL_GROUPBY_OPERATOR_H
