#pragma once

#include <execution_graph//Context.h>
#include <string>
#include <vector>
#include <tuple>

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
	COUNT_ALL,
	COUNT_DISTINCT
};

namespace ral {
namespace operators {

	std::tuple<std::vector<int>, std::vector<std::string>, std::vector<AggregateKind>, std::vector<std::string>> 
		parseGroupByExpression(const std::string & queryString, std::size_t num_cols);

	std::tuple<std::vector<int>, std::vector<std::string>, std::vector<AggregateKind>, std::vector<std::string>> 
		modGroupByParametersForMerge(const std::vector<int> & group_column_indices, 
		const std::vector<AggregateKind> & aggregation_types, const std::vector<std::string> & merging_column_names);

	std::unique_ptr<ral::frame::BlazingTable> compute_groupby_without_aggregations(
		const ral::frame::BlazingTableView & table, const std::vector<int> & group_column_indices);

	std::unique_ptr<ral::frame::BlazingTable> compute_aggregations_without_groupby(
		const ral::frame::BlazingTableView & table, const std::vector<std::string> & aggregation_input_expressions, 
		const std::vector<AggregateKind> & aggregation_types, const std::vector<std::string> & aggregation_column_assigned_aliases);

	std::unique_ptr<ral::frame::BlazingTable> compute_aggregations_with_groupby(
		const ral::frame::BlazingTableView & table, const std::vector<std::string> & aggregation_input_expressions, const std::vector<AggregateKind> & aggregation_types,
		const std::vector<std::string> & aggregation_column_assigned_aliases, const std::vector<int> & group_column_indices);

}  // namespace operators
}  // namespace ral
