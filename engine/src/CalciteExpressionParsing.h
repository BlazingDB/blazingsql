#pragma once

#include "cudf/types.hpp"
#include <string>
#include <vector>

bool is_type_float(cudf::type_id type);
bool is_type_integer(cudf::type_id type);
bool is_type_bool(cudf::type_id type) ;
bool is_type_timestamp(cudf::type_id type);
bool is_type_string(cudf::type_id type);

cudf::size_type get_index(const std::string & operand_string);

// interprets the expression and if is n-ary and logical, then returns their corresponding binary version
std::string expand_if_logical_op(std::string expression);

std::string clean_calcite_expression(const std::string & expression);

std::vector<std::string> get_tokens_in_reverse_order(const std::string & expression);

std::string get_aggregation_operation_string(std::string operator_expression);

std::string get_string_between_outer_parentheses(std::string operator_string);

std::unique_ptr<cudf::scalar> get_scalar_from_string(const std::string & scalar_string, cudf::data_type type);

int count_string_occurrence(std::string haystack, std::string needle);
