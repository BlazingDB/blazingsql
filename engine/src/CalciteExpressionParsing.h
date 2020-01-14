/*
 * CalciteExpressionParsing.h
 *
 *  Created on: Aug 7, 2018
 *      Author: felipe
 */

#ifndef CALCITEEXPRESSIONPARSING_H_
#define CALCITEEXPRESSIONPARSING_H_

#include "cudf/legacy/binaryop.hpp"
#include "cudf/types.h"
#include "gdf_wrapper/gdf_wrapper.cuh"
#include "parser/expression_utils.hpp"
#include <string>
#include <vector>
#include "execution_graph/logic_controllers/LogicPrimitives.h"
class blazing_frame;

gdf_binary_operator_exp get_binary_operation(std::string operator_string);

gdf_unary_operator get_unary_operation(std::string operator_string);

cudf::size_type get_index(const std::string & operand_string);

// interprets the expression and if is n-ary and logical, then returns their corresponding binary version
std::string expand_if_logical_op(std::string expression);

std::string clean_calcite_expression(const std::string & expression);

std::vector<std::string> get_tokens_in_reverse_order(const std::string & expression);

// NOTE call this function after use get_tokens_in_reverse_order ... TODO refactos this approach
void fix_tokens_after_call_get_tokens_in_reverse_order_for_timestamp(
	const ral::frame::BlazingTableView & table, std::vector<std::string> & tokens);

gdf_agg_op get_aggregation_operation(std::string operator_string);

std::string get_string_between_outer_parentheses(std::string operator_string);

cudf::type_id infer_dtype_from_literal(const std::string & token);

cudf::type_id get_output_type_expression(const ral::frame::BlazingTableView & table, cudf::type_id * max_temp_type, std::string expression);

cudf::type_id get_aggregation_output_type(cudf::type_id input_type, gdf_agg_op aggregation, bool have_groupby);

std::unique_ptr<cudf::scalar> get_scalar_from_string(const std::string & scalar_string);

std::string aggregator_to_string(gdf_agg_op operation);

// takes an expression and given a starting index pointing at either ( or [, it finds the corresponding closing char )
// or ]
int find_closing_char(const std::string & expression, int start);

// takes a comma delimited list of expressions and splits it into separate expressions
// if the flag trim is true, leading and trailing spaces are removed
std::vector<std::string> get_expressions_from_expression_list(std::string & combined_expressions, bool trim = true);

bool is_type_signed(cudf::type_id type);
bool is_type_float(cudf::type_id type);
bool is_type_integer(cudf::type_id type);
bool is_date_type(cudf::type_id type);
bool is_numeric_type(cudf::type_id type);

cudf::type_id get_output_type(cudf::type_id input_left_type, cudf::type_id input_right_type, gdf_binary_operator_exp operation);
cudf::type_id get_output_type(cudf::type_id input_left_type, gdf_unary_operator operation);

// this function takes two data types and returns the a common data type that the both can be losslessly be converted to
// the function returns true if a common type is possible, or false if there is no common type
// this function assumes that common types are decimal, float, datetime and string. You cannot convert across these
// general types.
void get_common_type(cudf::type_id type1,
	cudf::type_id type2,
	cudf::type_id & type_out);

std::string get_named_expression(std::string query_part, std::string expression_name);
std::string get_filter_expression(std::string query_part);

bool contains_evaluation(std::string expression);

#endif /* CALCITEEXPRESSIONPARSING_H_ */
