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
#include "parser/expression_utils.hpp"
#include <string>
#include <vector>
#include <cudf/detail/aggregation/aggregation.hpp>
#include <cudf/reduction.hpp>
#include "execution_graph/logic_controllers/LogicPrimitives.h"
#include "operators/GroupBy.h"

cudf::size_type get_index(const std::string & operand_string);

// interprets the expression and if is n-ary and logical, then returns their corresponding binary version
std::string expand_if_logical_op(std::string expression);

std::string replace_calcite_regex(const std::string & expression);

std::string clean_calcite_expression(const std::string & expression);

std::vector<std::string> get_tokens_in_reverse_order(const std::string & expression);

// NOTE call this function after use get_tokens_in_reverse_order ... TODO refactos this approach
void fix_tokens_after_call_get_tokens_in_reverse_order_for_timestamp(
	const cudf::table_view & inputs, std::vector<std::string> & tokens);

std::string get_aggregation_operation_string(std::string operator_expression);

AggregateKind get_aggregation_operation(std::string operator_string);

std::string get_string_between_outer_parentheses(std::string operator_string);

cudf::type_id infer_dtype_from_literal(const std::string & token);

cudf::type_id get_output_type_expression(const cudf::table_view & table, std::string expression);

cudf::type_id get_aggregation_output_type(cudf::type_id input_type, AggregateKind aggregation, bool have_groupby);

cudf::type_id get_aggregation_output_type(cudf::type_id input_type, const std::string & aggregation);

std::unique_ptr<cudf::scalar> get_scalar_from_string(const std::string & scalar_string);
std::unique_ptr<cudf::scalar> get_scalar_from_string(const std::string & scalar_string, const cudf::type_id & type_id);

std::string aggregator_to_string(AggregateKind operation);

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

// this function takes two data types and returns the a common data type that the both can be losslessly be converted to
// the function returns cudf::type_id::EMPTY if there is no common type
// this function assumes that common types are decimal, float, datetime and string. You cannot convert across these
// general types.
cudf::type_id get_common_type(cudf::type_id type1, cudf::type_id type2);

bool contains_evaluation(std::string expression);

int count_string_occurrence(std::string haystack, std::string needle);

#endif /* CALCITEEXPRESSIONPARSING_H_ */
