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
#include <map>
#include <memory>
#include "parser/expression_tree.hpp"

namespace interops {

typedef int16_t column_index_type;

enum column_index : column_index_type {
	UNARY_INDEX = -1,
	SCALAR_INDEX = -2,
	SCALAR_NULL_INDEX = -3,
	NULLARY_INDEX = -4

};

void add_expression_to_interpreter_plan(const ral::parser::parse_tree & expr_tree,
	const std::map<column_index_type, column_index_type> & expr_idx_to_col_idx_map,
	cudf::size_type start_processing_position,
	cudf::size_type final_output_position,
	std::vector<column_index_type> & left_inputs,
	std::vector<column_index_type> & right_inputs,
	std::vector<column_index_type> & outputs,
	std::vector<operator_type> & operators,
	std::vector<std::unique_ptr<cudf::scalar>> & left_scalars,
	std::vector<std::unique_ptr<cudf::scalar>> & right_scalars);

void perform_interpreter_operation(cudf::mutable_table_view & out_table,
	const cudf::table_view & table,
	const std::vector<column_index_type> & left_inputs,
	const std::vector<column_index_type> & right_inputs,
	const std::vector<column_index_type> & outputs,
	const std::vector<column_index_type> & final_output_positions,
	const std::vector<operator_type> & operators,
	const std::vector<std::unique_ptr<cudf::scalar>> & left_scalars,
	const std::vector<std::unique_ptr<cudf::scalar>> & right_scalars,
	cudf::size_type operation_num_rows = 0);

} // namespace interops
