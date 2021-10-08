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

	inline std::unique_ptr<cudf::aggregation> makeCudfAggregation(AggregateKind input, int offset = 0){
    switch(input) {
		case AggregateKind::SUM: {
			return cudf::make_sum_aggregation<cudf::aggregation>();
    }
		case AggregateKind::MEAN: {
			return cudf::make_mean_aggregation<cudf::aggregation>();
    }
		case AggregateKind::MIN: {
			return cudf::make_min_aggregation<cudf::aggregation>();
    }
		case AggregateKind::MAX: {
			return cudf::make_max_aggregation<cudf::aggregation>();
    }
		case AggregateKind::COUNT_VALID: {
			return cudf::make_count_aggregation<cudf::aggregation>(cudf::null_policy::EXCLUDE);
    }
		case AggregateKind::COUNT_ALL: {
			return cudf::make_count_aggregation<cudf::aggregation>(cudf::null_policy::INCLUDE);
    }
		case AggregateKind::SUM0: {
			return cudf::make_sum_aggregation<cudf::aggregation>();
    }
		case AggregateKind::NTH_ELEMENT: {
			// TODO: https://github.com/BlazingDB/blazingsql/issues/1531
			return cudf::make_nth_element_aggregation<cudf::aggregation>(offset, cudf::null_policy::INCLUDE);
    }
		case AggregateKind::COUNT_DISTINCT: {
			/* Currently this conditional is unreachable.
			Calcite transforms count distincts through the
			AggregateExpandDistinctAggregates rule, so in fact,
			each count distinct is replaced by some group by clauses. */
			return cudf::make_nunique_aggregation<cudf::aggregation>();
		}
    default:
      throw std::runtime_error(
        "In makeCudfGroupbyAggregation function: AggregateKind type not supported");
      }
	}

	inline std::unique_ptr<cudf::groupby_aggregation> makeCudfGroupbyAggregation(AggregateKind input, int offset = 0){
    switch(input) {
		case AggregateKind::SUM: {
			return cudf::make_sum_aggregation<cudf::groupby_aggregation>();
    }
		case AggregateKind::MEAN: {
			return cudf::make_mean_aggregation<cudf::groupby_aggregation>();
    }
		case AggregateKind::MIN: {
			return cudf::make_min_aggregation<cudf::groupby_aggregation>();
    }
		case AggregateKind::MAX: {
			return cudf::make_max_aggregation<cudf::groupby_aggregation>();
    }
		case AggregateKind::COUNT_VALID: {
			return cudf::make_count_aggregation<cudf::groupby_aggregation>(cudf::null_policy::EXCLUDE);
    }
		case AggregateKind::COUNT_ALL: {
			return cudf::make_count_aggregation<cudf::groupby_aggregation>(cudf::null_policy::INCLUDE);
    }
		case AggregateKind::SUM0: {
			return cudf::make_sum_aggregation<cudf::groupby_aggregation>();
    }
		case AggregateKind::NTH_ELEMENT: {
			// TODO: https://github.com/BlazingDB/blazingsql/issues/1531
			return cudf::make_nth_element_aggregation<cudf::groupby_aggregation>(offset, cudf::null_policy::INCLUDE);
    }
		case AggregateKind::COUNT_DISTINCT: {
			/* Currently this conditional is unreachable.
			Calcite transforms count distincts through the
			AggregateExpandDistinctAggregates rule, so in fact,
			each count distinct is replaced by some group by clauses. */
			return cudf::make_nunique_aggregation<cudf::groupby_aggregation>();
		}
    default:
      throw std::runtime_error(
        "In makeCudfGroupbyAggregation function: AggregateKind type not supported");
      }
	}

	// offset param is needed for `LAG` and `LEAD` aggs
	inline std::unique_ptr<cudf::rolling_aggregation> makeCudfRollingAggregation(AggregateKind input, int offset = 0){
    switch(input) {
      case AggregateKind::SUM: {
        return cudf::make_sum_aggregation<cudf::rolling_aggregation>();
      }
      case AggregateKind::MEAN: {
        return cudf::make_mean_aggregation<cudf::rolling_aggregation>();
      }
      case AggregateKind::MIN: {
        return cudf::make_min_aggregation<cudf::rolling_aggregation>();
      }
      case AggregateKind::MAX: {
        return cudf::make_max_aggregation<cudf::rolling_aggregation>();
      }
      case AggregateKind::COUNT_VALID: {
        return cudf::make_count_aggregation<cudf::rolling_aggregation>(cudf::null_policy::EXCLUDE);
      }
      case AggregateKind::COUNT_ALL: {
        return cudf::make_count_aggregation<cudf::rolling_aggregation>(cudf::null_policy::INCLUDE);
      }
      case AggregateKind::SUM0: {
        return cudf::make_sum_aggregation<cudf::rolling_aggregation>();
      }
      case AggregateKind::ROW_NUMBER:  {
        return cudf::make_row_number_aggregation<cudf::rolling_aggregation>();
      }
      case AggregateKind::LAG: {
        return cudf::make_lag_aggregation<cudf::rolling_aggregation>(offset);
      }
      case AggregateKind::LEAD: {
        return cudf::make_lead_aggregation<cudf::rolling_aggregation>(offset);
      }
      default:
        throw std::runtime_error(
          "In makeCudfRollingAggregation function: AggregateKind type not supported");
    }
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
