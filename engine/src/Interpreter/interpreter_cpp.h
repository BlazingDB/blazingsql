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

/**
 * @brief Encodes an expression tree consisting of simple operations in a GPU
 * friendly format that we can later evaluate in a single GPU kernel call
 *
 * The interpreter can only evaluate simple operations like arithmetic
 * operations, cast, etc on primitives types. Any complex operation inside
 * the expression tree must be removed (or evaluated and replaced by its result)
 * in a preprocess step
 *
 * @param expr_tree The expression tree to encode
 * @param expr_idx_to_col_idx_map A map from input table column indices to expression
 * left/right input indices
 * @param start_processing_position The start position from where to store temp results
 * in the encoded plan
 * @param final_output_position The ouput position in the encoded plan for this expression
 * @param left_inputs The encoded left inputs indices from all the processed expressions
 * @param right_inputs The encoded right inputs indices from all the processed expressions
 * @param outputs The encoded output indices from all the processed expressions
 * @param operators The encoded operations from all the processed expressions
 * @param left_scalars The scalars used as left inputs in the operations
 * @param right_scalars The scalars used as right inputs in the operations
 */
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

/**
 * @brief Evaluates multiple operations encoded in a GPU friendly format in a
 * single GPU kernel call
 *
 * @param out_table The output table to store the results
 * @param table The input table
 * @param left_inputs The encoded left inputs indices from all the processed expressions
 * @param right_inputs The encoded right inputs indices from all the processed expressions
 * @param outputs The encoded output indices from all the processed expressions
 * @param final_output_positions The encoded final output indices for all the processed expressions
 * @param operators The encoded operations from all the processed expressions
 * @param left_scalars The scalars used as left inputs in the operations
 * @param right_scalars The scalars used as right inputs in the operations
 * @param operation_num_rows The output number of rows
 *
 */
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
