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

namespace interops {

typedef int16_t column_index_type;

enum column_index : column_index_type {
	UNARY_INDEX = -1,
	SCALAR_INDEX = -2,
	SCALAR_NULL_INDEX = -3
};

enum class operator_type {
	BLZ_INVALID_OP,

	// Unary operators
	BLZ_NOT,
	BLZ_ABS,
	BLZ_FLOOR,
	BLZ_CEIL,
	BLZ_SIN,
	BLZ_COS,
	BLZ_ASIN,
	BLZ_ACOS,
	BLZ_TAN,
	BLZ_COTAN,
	BLZ_ATAN,
	BLZ_LN,
	BLZ_LOG,
	BLZ_YEAR,
	BLZ_MONTH,
	BLZ_DAY,
	BLZ_HOUR,
	BLZ_MINUTE,
	BLZ_SECOND,
	BLZ_IS_NULL,
	BLZ_IS_NOT_NULL,
	BLZ_CAST_INTEGER,
	BLZ_CAST_BIGINT,
	BLZ_CAST_FLOAT,
	BLZ_CAST_DOUBLE,
	BLZ_CAST_DATE,
	BLZ_CAST_TIMESTAMP,
	BLZ_CAST_VARCHAR,

	// Binary operators
  BLZ_ADD,            ///< operator +
  BLZ_SUB,            ///< operator -
  BLZ_MUL,            ///< operator *
  BLZ_DIV,            ///< operator / using common type of lhs and rhs
  BLZ_MOD,            ///< operator %
  BLZ_POW,            ///< lhs ^ rhs
  BLZ_ROUND,
  BLZ_EQUAL,          ///< operator ==
  BLZ_NOT_EQUAL,      ///< operator !=
  BLZ_LESS,           ///< operator <
  BLZ_GREATER,        ///< operator >
  BLZ_LESS_EQUAL,     ///< operator <=
  BLZ_GREATER_EQUAL,  ///< operator >=
  BLZ_BITWISE_AND,    ///< operator &
  BLZ_BITWISE_OR,     ///< operator |
  BLZ_BITWISE_XOR,    ///< operator ^
  BLZ_LOGICAL_AND,    ///< operator &&
  BLZ_LOGICAL_OR,     ///< operator ||
	BLZ_FIRST_NON_MAGIC,
	BLZ_MAGIC_IF_NOT,
	BLZ_STR_LIKE,
	BLZ_STR_SUBSTRING,
	BLZ_STR_CONCAT
};

bool is_unary_operator(operator_type op);

bool is_binary_operator(operator_type op);

cudf::type_id get_output_type(cudf::type_id input_left_type, operator_type op);

cudf::type_id get_output_type(cudf::type_id input_left_type, cudf::type_id input_right_type, operator_type op);

void add_expression_to_interpreter_plan(const std::vector<std::string> & tokenized_expression,
	const cudf::table_view & table,
	const std::map<column_index_type, column_index_type> & expr_idx_to_col_idx_map,
	cudf::size_type expression_position,
	cudf::size_type num_total_outputs,
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
	const std::vector<std::unique_ptr<cudf::scalar>> & right_scalars);

} // namespace interops
