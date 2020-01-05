/*
 * JoinProcessor.cpp
 *
 *  Created on: Aug 7, 2018
 *      Author: felipe
 */

#include "CalciteExpressionParsing.h"
#include "LogicalFilter.h"
//#include <cub/cub.cuh>
#include "DataFrame.h"
#include "utilities/CommonOperations.h"
#include <algorithm>
#include <cudf/legacy/join.hpp>
#include <numeric>
#include <stack>
#include <thrust/device_ptr.h>
#include <thrust/device_vector.h>
#include <thrust/functional.h>
#include <thrust/transform.h>

// based on calcites relational algebra
const std::string INNER_JOIN = "inner";
const std::string LEFT_JOIN = "left";
const std::string RIGHT_JOIN = "right";
const std::string OUTER_JOIN = "full";

void parseJoinConditionToColumnIndices(const std::string & condition, std::vector<int> & columnIndices) {
	// TODO: right now this only works for equijoins
	// since this is all that is implemented at the time

	// TODO: for this to work properly we can only do multi column join
	// when we have ands, when we have hors we hvae to perform the joisn seperately then
	// do a unique merge of the indices

	// right now with pred push down the join codnition takes the filters as the second argument to condition

	std::string clean_expression = clean_calcite_expression(condition);
	int operator_count = 0;
	std::stack<std::string> operand;
	// NOTE percy c.cordoba here we dont need to call fix_tokens_after_call_get_tokens_in_reverse_order_for_timestamp
	// after
	std::vector<std::string> tokens = get_tokens_in_reverse_order(clean_expression);
	for(std::string token : tokens) {
		if(is_operator_token(token)) {
			if(token == "=") {
				// so far only equijoins are supported in libgdf
				operator_count++;
			} else if(token != "AND") {
				throw std::runtime_error("In evaluate_join function: unsupported non-equijoins operator");
			}
		} else {
			operand.push(token);
		}
	}

	columnIndices.resize(2 * operator_count);
	for(size_t i = 0; i < operator_count; i++) {
		int right_index = get_index(operand.top());
		operand.pop();
		int left_index = get_index(operand.top());
		operand.pop();

		if(right_index < left_index) {
			std::swap(left_index, right_index);
		}

		columnIndices[2 * i] = left_index;
		columnIndices[2 * i + 1] = right_index;
	}
}

void evaluate_join(std::string condition,
	std::string join_type,
	blazing_frame data_frame,
	cudf::column * left_result,
	cudf::column * right_result) {
	// TODO: right now this only works for equijoins
	// since this is all that is implemented at the time

	// TODO: for this to work properly we can only do multi column join
	// when we have ands, when we have hors we hvae to perform the joisn seperately then
	// do a unique merge of the indices

	// right now with pred push down the join codnition takes the filters as the second argument to condition

	std::vector<int> column_indices;
	parseJoinConditionToColumnIndices(condition, column_indices);

	int operator_count = column_indices.size() / 2;
	std::vector<cudf::column *> left_columns(operator_count);
	std::vector<cudf::column *> right_columns(operator_count);
	std::vector<std::vector<gdf_column_cpp>> normalized_join_columns(operator_count);
	for(int i = 0; i < operator_count; i++) {
		normalized_join_columns[i].push_back(data_frame.get_column(column_indices[2 * i]));
		normalized_join_columns[i].push_back(data_frame.get_column(column_indices[2 * i + 1]));
		normalized_join_columns[i] = ral::utilities::normalizeColumnTypes(normalized_join_columns[i]);
		left_columns[i] = normalized_join_columns[i][0].get_gdf_column();
		right_columns[i] = normalized_join_columns[i][1].get_gdf_column();
	}

	std::vector<int> join_cols(operator_count);
	std::iota(join_cols.begin(), join_cols.end(), 0);

	std::vector<std::pair<int, int>> columns_in_common(operator_count);
	for(int i = 0; i < operator_count; ++i) {
		columns_in_common[i].first = i;
		columns_in_common[i].second = i;
	}

	std::vector<cudf::column *> result_idx_cols = {left_result, right_result};
	
	// TODO percy cudf0.12 port to cudf::column
	//cudf::table result_idx_table(result_idx_cols);
	
	gdf_context ctxt{0, GDF_HASH, 0};
	if(join_type == INNER_JOIN) {
		// TODO percy cudf0.12 port to cudf::column
//		cudf::table result = cudf::inner_join(cudf::table{left_columns},
//			cudf::table{right_columns},
//			join_cols,
//			join_cols,
//			columns_in_common,
//			&result_idx_table,
//			&ctxt);
//		result.destroy();
	} else if(join_type == LEFT_JOIN) {
		// TODO percy cudf0.12 port to cudf::column
//		cudf::table result = cudf::left_join(cudf::table{left_columns},
//			cudf::table{right_columns},
//			join_cols,
//			join_cols,
//			columns_in_common,
//			&result_idx_table,
//			&ctxt);
//		result.destroy();
	} else if(join_type == OUTER_JOIN) {
		// TODO percy cudf0.12 port to cudf::column
//		cudf::table result = cudf::full_join(cudf::table{left_columns},
//			cudf::table{right_columns},
//			join_cols,
//			join_cols,
//			columns_in_common,
//			&result_idx_table,
//			&ctxt);
//		result.destroy();
	} else {
		throw std::runtime_error("In evaluate_join function: unsupported join operator, " + join_type);
	}
}
