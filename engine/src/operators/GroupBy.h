#pragma once

#include <execution_graph//Context.h>
#include <string>
#include <vector>
#include <tuple>

#include "execution_kernels/LogicPrimitives.h"
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
	COUNT_DISTINCT,
	ROW_NUMBER,
	LAG,
	LEAD,
	NTH_ELEMENT
};

namespace ral {
namespace operators {

	// offset param is needed for `LAG` and `LEAD` aggs
	template<typename cudf_aggregation_type_T>
	std::unique_ptr<cudf_aggregation_type_T> makeCudfAggregation(AggregateKind input, int offset = 0){
		if(input == AggregateKind::SUM){
			return cudf::make_sum_aggregation<cudf_aggregation_type_T>();
		}else if(input == AggregateKind::MEAN){
			return cudf::make_mean_aggregation<cudf_aggregation_type_T>();
		}else if(input == AggregateKind::MIN){
			return cudf::make_min_aggregation<cudf_aggregation_type_T>();
		}else if(input == AggregateKind::MAX){
			return cudf::make_max_aggregation<cudf_aggregation_type_T>();
		}else if(input == AggregateKind::ROW_NUMBER) {
			return cudf::make_row_number_aggregation<cudf_aggregation_type_T>();
		}else if(input == AggregateKind::COUNT_VALID){
			return cudf::make_count_aggregation<cudf_aggregation_type_T>(cudf::null_policy::EXCLUDE);
		}else if(input == AggregateKind::COUNT_ALL){
			return cudf::make_count_aggregation<cudf_aggregation_type_T>(cudf::null_policy::INCLUDE);
		}else if(input == AggregateKind::SUM0){
			return cudf::make_sum_aggregation<cudf_aggregation_type_T>();
		}else if(input == AggregateKind::LAG){
			return cudf::make_lag_aggregation<cudf_aggregation_type_T>(offset);	
		}else if(input == AggregateKind::LEAD){
			return cudf::make_lead_aggregation<cudf_aggregation_type_T>(offset);	
		}else if(input == AggregateKind::NTH_ELEMENT){
			// TODO: https://github.com/BlazingDB/blazingsql/issues/1531
			// return cudf::make_nth_element_aggregation<cudf_aggregation_type_T>(offset, cudf::null_policy::INCLUDE);	
		}else if(input == AggregateKind::COUNT_DISTINCT){
			/* Currently this conditional is unreachable.
			Calcite transforms count distincts through the
			AggregateExpandDistinctAggregates rule, so in fact,
			each count distinct is replaced by some group by clauses. */
			// return cudf::make_nunique_aggregation<cudf_aggregation_type_T>();
		}
		throw std::runtime_error(
			"In makeCudfAggregation function: AggregateKind type not supported");
	}

	AggregateKind get_aggregation_operation(std::string expression_in, bool is_window_operation = false);

	std::tuple<std::vector<int>, std::vector<std::string>, std::vector<AggregateKind>, std::vector<std::string>> 
		parseGroupByExpression(const std::string & queryString, std::size_t num_cols);

	std::tuple<std::vector<int>, std::vector<std::string>, std::vector<AggregateKind>, std::vector<std::string>> 
		modGroupByParametersPostComputeAggregations(const std::vector<int> & group_column_indices, 
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
