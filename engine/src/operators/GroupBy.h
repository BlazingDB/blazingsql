#ifndef BLAZINGDB_RAL_GROUPBY_OPERATOR_H
#define BLAZINGDB_RAL_GROUPBY_OPERATOR_H

#include <blazingdb/manager/Context.h>
#include <string>
#include <vector>

#include "execution_graph/logic_controllers/LogicPrimitives.h"
#include <cudf/aggregation.hpp>
#include <cudf/groupby.hpp>
#include <cudf/detail/aggregation/aggregation.hpp>

#include <cudf/column/column_view.hpp>

enum AggregateKind{
	SUM,
	SUM0,
	MEAN,
	MIN,
	MAX,
	COUNT_VALID,
	COUNT_ALL
};

namespace ral {
namespace operators {
namespace experimental {

	cudf::experimental::aggregation::Kind convertAggregationCudf(AggregateKind input);

	typedef blazingdb::manager::experimental::Context Context;

	std::unique_ptr<ral::frame::BlazingTable> process_aggregate(const ral::frame::BlazingTableView & table,
																const std::string& query_part, Context * context);

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
		const ral::frame::BlazingTableView & table, const std::vector<AggregateKind> & aggregation_types,
		const std::vector<std::string> & aggregation_input_expressions, const std::vector<std::string> & aggregation_column_assigned_aliases,
		const std::vector<int> & group_column_indices);


	// Bigger than GPU functions
	bool is_aggregations_without_groupby(const std::string& query_part);

	std::vector<int> get_group_column_indices(const std::string& query_part);

	std::pair<std::unique_ptr<ral::frame::BlazingTable>, std::unique_ptr<ral::frame::BlazingTable>>
	group_and_sample(const ral::frame::BlazingTableView & table,
									const std::string& query_part, Context * context);
	
	std::vector<std::unique_ptr<ral::frame::BlazingTable>>
	partition_group(const ral::frame::BlazingTableView & partitionPlan,
									const ral::frame::BlazingTableView & grouped_table,
									const std::string & query_part,
									Context * context);
	
	std::unique_ptr<ral::frame::BlazingTable>
	merge_group(const std::vector<ral::frame::BlazingTableView> & partitions_to_merge,
							const std::string & query_part,
							Context * context);

}  // namespace experimental
}  // namespace operators
}  // namespace ral

#endif  // BLAZINGDB_RAL_GROUPBY_OPERATOR_H
