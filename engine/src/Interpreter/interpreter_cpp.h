/*
 * interpreter_cpp.h
 *
 *  Created on: Jan 12, 2019
 *      Author: felipe
 */

#pragma once

#include <cudf/table/table_view.hpp>
#include <cudf/scalar/scalar.hpp>
#include <vector>
#include <memory>

#include "gdf_wrapper/gdf_types.cuh"

namespace interops {

typedef int16_t column_index_type;
static constexpr short UNARY_INDEX = -1;
static constexpr short SCALAR_INDEX = -2;
static constexpr short SCALAR_NULL_INDEX = -3;

void add_expression_to_interpreter_plan(const std::vector<std::string> & tokenized_expression,
	const cudf::table_view & table,
	const std::map<column_index_type, column_index_type> & expr_idx_to_col_idx_map,
	column_index_type expression_position,
	column_index_type num_total_outputs,
	std::vector<column_index_type> & left_inputs,
	std::vector<column_index_type> & right_inputs,
	std::vector<column_index_type> & outputs,
	std::vector<column_index_type> & final_output_positions,
	std::vector<gdf_binary_operator_exp> & operators,
	std::vector<gdf_unary_operator> & unary_operators,
	std::vector<std::unique_ptr<cudf::scalar>> & left_scalars,
	std::vector<std::unique_ptr<cudf::scalar>> & right_scalars);

void perform_interpreter_operation(cudf::mutable_table_view & out_table,
	const cudf::table_view & table,
	const std::vector<column_index_type> & left_inputs,
	const std::vector<column_index_type> & right_inputs,
	const std::vector<column_index_type> & outputs,
	const std::vector<column_index_type> & final_output_positions,
	const std::vector<gdf_binary_operator_exp> & operators,
	const std::vector<gdf_unary_operator> & unary_operators,
	const std::vector<std::unique_ptr<cudf::scalar>> & left_scalars,
	const std::vector<std::unique_ptr<cudf::scalar>> & right_scalars);

} // namespace interops
