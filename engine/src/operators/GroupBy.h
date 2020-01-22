#ifndef BLAZINGDB_RAL_GROUPBY_OPERATOR_H
#define BLAZINGDB_RAL_GROUPBY_OPERATOR_H

#include "DataFrame.h"
#include <blazingdb/manager/Context.h>
#include <string>
#include <vector>

#include "execution_graph/logic_controllers/LogicPrimitives.h"

#include <cudf/aggregation.hpp>
#include <cudf/groupby.hpp>

#include <cudf/detail/aggregation/aggregation.hpp>

#include <cudf/column/column_view.hpp>

namespace ral {
namespace operators {



namespace {
using blazingdb::manager::Context;
}  // namespace

bool is_aggregate(std::string query_part);

std::unique_ptr<ral::frame::BlazingTable> process_aggregate(const ral::frame::BlazingTableView & table, std::string query_part, Context * queryContext);

// TODO rommel percy
std::unique_ptr<ral::frame::BlazingTable> groupby_without_aggregations(
	const ral::frame::BlazingTableView & table, const std::vector<int> & group_column_indices);

void aggregations_with_groupby(std::vector<CudfColumnView> const & group_by_columns,

   std::vector<CudfColumnView> const & aggregation_inputs,

   std::vector<cudf::experimental::aggregation::Kind> const & agg_ops,
   std::vector< std::unique_ptr<cudf::column> > & group_by_output_columns,
   std::vector< std::unique_ptr<cudf::column> > & aggregation_output_columns);


}  // namespace operators
}  // namespace ral


namespace ral {
namespace operators {
namespace experimental {

	typedef blazingdb::manager::experimental::Context Context;

	std::unique_ptr<ral::frame::BlazingTable> process_aggregate(const ral::frame::BlazingTableView & table,
																std::string query_part, Context * context);

	std::unique_ptr<ral::frame::BlazingTable> groupby_without_aggregations(Context * context,
		const ral::frame::BlazingTableView & table, const std::vector<int> & group_column_indices);

	std::unique_ptr<ral::frame::BlazingTable> compute_groupby_without_aggregations(
		const ral::frame::BlazingTableView & table, const std::vector<int> & group_column_indices);
	
	std::unique_ptr<ral::frame::BlazingTable> aggregations_without_groupby(Context * context,
		const ral::frame::BlazingTableView & table, const std::vector<std::string> & aggregation_expressions,
		const std::vector<std::string> & aggregation_column_assigned_aliases);

	std::unique_ptr<ral::frame::BlazingTable> compute_aggregations_without_groupby(
		const ral::frame::BlazingTableView & table, const std::vector<std::string> &  aggregation_types, 
		const std::vector<std::string> & aggregation_input_expressions, const std::vector<std::string> & aggregation_column_assigned_aliases);

	std::unique_ptr<ral::frame::BlazingTable> aggregations_with_groupby(Context * context,
		const ral::frame::BlazingTableView & table, const std::vector<std::string> & aggregation_expressions,
		const std::vector<std::string> & aggregation_column_assigned_aliases, const std::vector<int> & group_column_indices);

	std::unique_ptr<ral::frame::BlazingTable> compute_aggregations_with_groupby(
		const ral::frame::BlazingTableView & table, const std::vector<cudf::experimental::aggregation::Kind> & aggregation_types, 
		const std::vector<std::string> & aggregation_input_expressions, const std::vector<std::string> & aggregation_column_assigned_aliases, 
		const std::vector<int> & group_column_indices);

}  // namespace experimental
}  // namespace operators
}  // namespace ral

#endif  // BLAZINGDB_RAL_GROUPBY_OPERATOR_H
