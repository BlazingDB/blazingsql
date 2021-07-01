#include <map>
#include <regex>
#include <cassert>
#include <blazingdb/io/Util/StringUtil.h>

#include "expression_utils.hpp"
#include "parser/CalciteExpressionParsing.h"
#include "utilities/error.hpp"

bool is_nullary_operator(operator_type op){
switch (op)
	{
	case operator_type::BLZ_RAND:
		return true;
	default:
		return false;
	}
}

bool is_unary_operator(operator_type op) {
	switch (op)
	{
	case operator_type::BLZ_NOT:
	case operator_type::BLZ_IS_TRUE:
	case operator_type::BLZ_ABS:
	case operator_type::BLZ_FLOOR:
	case operator_type::BLZ_CEIL:
	case operator_type::BLZ_SIN:
	case operator_type::BLZ_COS:
	case operator_type::BLZ_ASIN:
	case operator_type::BLZ_ACOS:
	case operator_type::BLZ_TAN:
	case operator_type::BLZ_COTAN:
	case operator_type::BLZ_ATAN:
	case operator_type::BLZ_LN:
	case operator_type::BLZ_LOG:
	case operator_type::BLZ_YEAR:
	case operator_type::BLZ_MONTH:
	case operator_type::BLZ_DAY:
	case operator_type::BLZ_DAYOFWEEK:
	case operator_type::BLZ_HOUR:
	case operator_type::BLZ_MINUTE:
	case operator_type::BLZ_SECOND:
	case operator_type::BLZ_IS_NULL:
	case operator_type::BLZ_IS_NOT_NULL:
	case operator_type::BLZ_IS_NOT_TRUE:
	case operator_type::BLZ_IS_NOT_FALSE:
	case operator_type::BLZ_CAST_TINYINT:
	case operator_type::BLZ_CAST_SMALLINT:
	case operator_type::BLZ_CAST_INTEGER:
	case operator_type::BLZ_CAST_BIGINT:
	case operator_type::BLZ_CAST_FLOAT:
	case operator_type::BLZ_CAST_DOUBLE:
	case operator_type::BLZ_CAST_DATE:
	case operator_type::BLZ_CAST_TIMESTAMP_SECONDS:
	case operator_type::BLZ_CAST_TIMESTAMP_MILLISECONDS:
	case operator_type::BLZ_CAST_TIMESTAMP_MICROSECONDS:
	case operator_type::BLZ_CAST_TIMESTAMP:
	case operator_type::BLZ_CAST_VARCHAR:
	case operator_type::BLZ_CHAR_LENGTH:
	case operator_type::BLZ_STR_LOWER:
	case operator_type::BLZ_STR_UPPER:
	case operator_type::BLZ_STR_INITCAP:
	case operator_type::BLZ_STR_REVERSE:
		return true;
	default:
		return false;
	}
}

bool is_binary_operator(operator_type op) {
	switch (op)
	{
	case operator_type::BLZ_ADD:
	case operator_type::BLZ_SUB:
	case operator_type::BLZ_MUL:
	case operator_type::BLZ_DIV:
	case operator_type::BLZ_MOD:
	case operator_type::BLZ_POW:
	case operator_type::BLZ_ROUND:
	case operator_type::BLZ_EQUAL:
	case operator_type::BLZ_NOT_EQUAL:
	case operator_type::BLZ_LESS:
	case operator_type::BLZ_GREATER:
	case operator_type::BLZ_LESS_EQUAL:
	case operator_type::BLZ_GREATER_EQUAL:
	case operator_type::BLZ_BITWISE_AND:
	case operator_type::BLZ_BITWISE_OR:
	case operator_type::BLZ_BITWISE_XOR:
	case operator_type::BLZ_LOGICAL_AND:
	case operator_type::BLZ_LOGICAL_OR:
	case operator_type::BLZ_FIRST_NON_MAGIC:
	case operator_type::BLZ_MAGIC_IF_NOT:
	case operator_type::BLZ_STR_LIKE:
	case operator_type::BLZ_STR_CONCAT:
	case operator_type::BLZ_STR_LEFT:
	case operator_type::BLZ_STR_RIGHT:
	case operator_type::BLZ_IS_NOT_DISTINCT_FROM:
		return true;
	case operator_type::BLZ_STR_SUBSTRING:
	case operator_type::BLZ_STR_REPLACE:
	case operator_type::BLZ_TO_DATE:
	case operator_type::BLZ_TO_TIMESTAMP:
	case operator_type::BLZ_STR_TRIM:
	case operator_type::BLZ_STR_REGEXP_REPLACE:
		assert(false);
		// Ternary operator. Should not reach here
		// Should be evaluated in place (inside function_evaluator_transformer) and removed from the tree
		return true;
	default:
		return false;
	}
}

cudf::type_id get_output_type(operator_type op) {
	switch (op)
	{
	case operator_type::BLZ_RAND:
		return cudf::type_id::FLOAT64;
	default:
	 	assert(false);
		return cudf::type_id::EMPTY;
	}
}

cudf::type_id get_output_type(operator_type op, cudf::type_id input_left_type) {
	switch (op)
	{
	case operator_type::BLZ_CAST_TINYINT:
		return cudf::type_id::INT8;
	case operator_type::BLZ_CAST_SMALLINT:
		return cudf::type_id::INT16;
	case operator_type::BLZ_CAST_INTEGER:
		return cudf::type_id::INT32;
	case operator_type::BLZ_CAST_BIGINT:
		return cudf::type_id::INT64;
	case operator_type::BLZ_CAST_FLOAT:
		return cudf::type_id::FLOAT32;
	case operator_type::BLZ_CAST_DOUBLE:
		return cudf::type_id::FLOAT64;
	case operator_type::BLZ_CAST_DATE:
		return cudf::type_id::TIMESTAMP_DAYS;
	case operator_type::BLZ_CAST_TIMESTAMP_SECONDS:
		return cudf::type_id::TIMESTAMP_SECONDS;
	case operator_type::BLZ_CAST_TIMESTAMP_MILLISECONDS:
		return cudf::type_id::TIMESTAMP_MILLISECONDS;
	case operator_type::BLZ_CAST_TIMESTAMP_MICROSECONDS:
		return cudf::type_id::TIMESTAMP_MICROSECONDS;
	case operator_type::BLZ_CAST_TIMESTAMP:
		return cudf::type_id::TIMESTAMP_NANOSECONDS;
	case operator_type::BLZ_STR_LOWER:
	case operator_type::BLZ_STR_UPPER:
	case operator_type::BLZ_STR_INITCAP:
	case operator_type::BLZ_STR_REVERSE:
	case operator_type::BLZ_STR_LEFT:
	case operator_type::BLZ_STR_RIGHT:
	case operator_type::BLZ_CAST_VARCHAR:
		return cudf::type_id::STRING;
	case operator_type::BLZ_YEAR:
	case operator_type::BLZ_MONTH:
	case operator_type::BLZ_DAY:
	case operator_type::BLZ_DAYOFWEEK:
	case operator_type::BLZ_HOUR:
	case operator_type::BLZ_MINUTE:
	case operator_type::BLZ_SECOND:
		return cudf::type_id::INT16;
	case operator_type::BLZ_SIN:
	case operator_type::BLZ_COS:
	case operator_type::BLZ_ASIN:
	case operator_type::BLZ_ACOS:
	case operator_type::BLZ_TAN:
	case operator_type::BLZ_COTAN:
	case operator_type::BLZ_ATAN:
	case operator_type::BLZ_LN:
	case operator_type::BLZ_LOG:
	case operator_type::BLZ_FLOOR:
	case operator_type::BLZ_CEIL:
		if(is_type_float(input_left_type)) {
			return input_left_type;
		} else {
			return cudf::type_id::FLOAT64;
		}
	case operator_type::BLZ_ABS:
		return input_left_type;
	case operator_type::BLZ_NOT:
	case operator_type::BLZ_IS_TRUE:
	case operator_type::BLZ_IS_NULL:
	case operator_type::BLZ_IS_NOT_NULL:
	case operator_type::BLZ_IS_NOT_TRUE:
	case operator_type::BLZ_IS_NOT_FALSE:
	case operator_type::BLZ_IS_NOT_DISTINCT_FROM:
		return cudf::type_id::BOOL8;
	case operator_type::BLZ_CHAR_LENGTH:
		return cudf::type_id::INT32;
	default:
	 	assert(false);
		return cudf::type_id::EMPTY;
	}
}

cudf::type_id get_output_type(operator_type op, cudf::type_id input_left_type, cudf::type_id input_right_type) {
	RAL_EXPECTS(input_left_type != cudf::type_id::EMPTY || input_right_type != cudf::type_id::EMPTY, "In get_output_type function: both operands types are empty");

	if(input_left_type == cudf::type_id::EMPTY) {
		input_left_type = input_right_type;
	} else if(input_right_type == cudf::type_id::EMPTY) {
		input_right_type = input_left_type;
	}

	switch (op)
	{
	case operator_type::BLZ_ADD:
	case operator_type::BLZ_SUB:
	case operator_type::BLZ_MUL:
	case operator_type::BLZ_DIV:
	case operator_type::BLZ_MOD:
		if(is_type_float(input_left_type) && is_type_float(input_right_type)) {
			return (cudf::size_of(cudf::data_type{input_left_type}) >= cudf::size_of(cudf::data_type{input_right_type}))
							? input_left_type
							: input_right_type;
		}	else if(is_type_float(input_left_type)) {
			return input_left_type;
		} else if(is_type_float(input_right_type)) {
			return input_right_type;
		} else {
			return (cudf::size_of(cudf::data_type{input_left_type}) >= cudf::size_of(cudf::data_type{input_right_type}))
							? input_left_type
							: input_right_type;
		}
	case operator_type::BLZ_EQUAL:
	case operator_type::BLZ_NOT_EQUAL:
	case operator_type::BLZ_LESS:
	case operator_type::BLZ_GREATER:
	case operator_type::BLZ_LESS_EQUAL:
	case operator_type::BLZ_GREATER_EQUAL:
	case operator_type::BLZ_LOGICAL_AND:
	case operator_type::BLZ_LOGICAL_OR:
		return cudf::type_id::BOOL8;
	case operator_type::BLZ_POW:
	case operator_type::BLZ_ROUND:
		return cudf::size_of(cudf::data_type{input_left_type}) <= cudf::size_of(cudf::data_type{cudf::type_id::FLOAT32})
					 ? cudf::type_id::FLOAT32
					 : cudf::type_id::FLOAT64;
	case operator_type::BLZ_MAGIC_IF_NOT:
		return input_right_type;
	case operator_type::BLZ_FIRST_NON_MAGIC:
		return (cudf::size_of(cudf::data_type{input_left_type}) >= cudf::size_of(cudf::data_type{input_right_type}))
				   ? input_left_type
				   : input_right_type;
	case operator_type::BLZ_STR_LIKE:
		return cudf::type_id::BOOL8;
	case operator_type::BLZ_STR_SUBSTRING:
	case operator_type::BLZ_STR_REPLACE:
	case operator_type::BLZ_STR_REGEXP_REPLACE:
	case operator_type::BLZ_STR_CONCAT:
	case operator_type::BLZ_STR_TRIM:
		return cudf::type_id::STRING;
	case operator_type::BLZ_TO_DATE:
		return cudf::type_id::TIMESTAMP_DAYS;
	case operator_type::BLZ_TO_TIMESTAMP:
		return cudf::type_id::TIMESTAMP_NANOSECONDS;
	default:
		assert(false);
		return cudf::type_id::EMPTY;
	}
}

operator_type map_to_operator_type(const std::string & operator_token) {
	static std::map<std::string, operator_type> OPERATOR_MAP = {
		// Nullary operators
		{"BLZ_RND", operator_type::BLZ_RAND},

		// Unary operators
		{"NOT", operator_type::BLZ_NOT},
		{"IS NOT TRUE", operator_type::BLZ_IS_NOT_TRUE},
		{"IS NOT FALSE", operator_type::BLZ_IS_NOT_FALSE},
		{"IS TRUE", operator_type::BLZ_IS_TRUE},
		{"SIN", operator_type::BLZ_SIN},
		{"ASIN", operator_type::BLZ_ASIN},
		{"COS", operator_type::BLZ_COS},
		{"ACOS", operator_type::BLZ_ACOS},
		{"TAN", operator_type::BLZ_TAN},
		{"ATAN", operator_type::BLZ_ATAN},
		{"FLOOR", operator_type::BLZ_FLOOR},
		{"CEIL", operator_type::BLZ_CEIL},
		{"ABS", operator_type::BLZ_ABS},
		{"LOG10", operator_type::BLZ_LOG},
		{"LN", operator_type::BLZ_LN},
		{"BL_YEAR", operator_type::BLZ_YEAR},
		{"BL_MONTH", operator_type::BLZ_MONTH},
		{"BL_DAY", operator_type::BLZ_DAY},
		{"BL_DOW", operator_type::BLZ_DAYOFWEEK},
		{"BL_HOUR", operator_type::BLZ_HOUR},
		{"BL_MINUTE", operator_type::BLZ_MINUTE},
		{"BL_SECOND", operator_type::BLZ_SECOND},
		{"IS_NULL", operator_type::BLZ_IS_NULL},
		{"IS_NOT_NULL", operator_type::BLZ_IS_NOT_NULL},
		{"CAST_TINYINT", operator_type::BLZ_CAST_TINYINT},
		{"CAST_SMALLINT", operator_type::BLZ_CAST_SMALLINT},
		{"CAST_INTEGER", operator_type::BLZ_CAST_INTEGER},
		{"CAST_BIGINT", operator_type::BLZ_CAST_BIGINT},
		{"CAST_FLOAT", operator_type::BLZ_CAST_FLOAT},
		{"CAST_DOUBLE", operator_type::BLZ_CAST_DOUBLE},
		{"CAST_DATE", operator_type::BLZ_CAST_DATE},
		{"CAST_TIMESTAMP_SECONDS", operator_type::BLZ_CAST_TIMESTAMP_SECONDS},
		{"CAST_TIMESTAMP_MILLISECONDS", operator_type::BLZ_CAST_TIMESTAMP_MILLISECONDS},
		{"CAST_TIMESTAMP_MICROSECONDS", operator_type::BLZ_CAST_TIMESTAMP_MICROSECONDS},
		{"CAST_TIMESTAMP", operator_type::BLZ_CAST_TIMESTAMP},
		{"CAST_VARCHAR", operator_type::BLZ_CAST_VARCHAR},
		{"CAST_CHAR", operator_type::BLZ_CAST_VARCHAR},
		{"CHAR_LENGTH", operator_type::BLZ_CHAR_LENGTH},
		{"LOWER", operator_type::BLZ_STR_LOWER},
		{"UPPER", operator_type::BLZ_STR_UPPER},
		{"INITCAP", operator_type::BLZ_STR_INITCAP},
		{"REVERSE", operator_type::BLZ_STR_REVERSE},

		// Binary operators
		{"=", operator_type::BLZ_EQUAL},
		{"<>", operator_type::BLZ_NOT_EQUAL},
		{">", operator_type::BLZ_GREATER},
		{">=", operator_type::BLZ_GREATER_EQUAL},
		{"<", operator_type::BLZ_LESS},
		{"<=", operator_type::BLZ_LESS_EQUAL},
		{"+", operator_type::BLZ_ADD},
		{"-", operator_type::BLZ_SUB},
		{"*", operator_type::BLZ_MUL},
		{"/", operator_type::BLZ_DIV},
		{"POWER", operator_type::BLZ_POW},
		{"ROUND", operator_type::BLZ_ROUND},
		{"MOD", operator_type::BLZ_MOD},
		{"AND", operator_type::BLZ_LOGICAL_AND},
		{"OR", operator_type::BLZ_LOGICAL_OR},
		{"FIRST_NON_MAGIC", operator_type::BLZ_FIRST_NON_MAGIC},
		{"MAGIC_IF_NOT", operator_type::BLZ_MAGIC_IF_NOT},
		{"LIKE", operator_type::BLZ_STR_LIKE},
		{"SUBSTRING", operator_type::BLZ_STR_SUBSTRING},
		{"REGEXP_REPLACE", operator_type::BLZ_STR_REGEXP_REPLACE},
		{"REPLACE", operator_type::BLZ_STR_REPLACE},
		{"TO_DATE", operator_type::BLZ_TO_DATE},
		{"TO_TIMESTAMP", operator_type::BLZ_TO_TIMESTAMP},
		{"||", operator_type::BLZ_STR_CONCAT},
		{"CONCAT", operator_type::BLZ_STR_CONCAT}, // we want to handle CONCAT op in the same way as ||
		{"TRIM", operator_type::BLZ_STR_TRIM},
		{"LEFT", operator_type::BLZ_STR_LEFT},
		{"RIGHT", operator_type::BLZ_STR_RIGHT},
		{"IS_NOT_DISTINCT_FROM", operator_type::BLZ_IS_NOT_DISTINCT_FROM},
	};

	RAL_EXPECTS(OPERATOR_MAP.find(operator_token) != OPERATOR_MAP.end(), "Unsupported operator: " + operator_token);

	return OPERATOR_MAP[operator_token];
}

bool is_null(const std::string & token) { return token == "null"; }

bool is_string(const std::string & token) { return token[0] == '\'' && token[token.size() - 1] == '\''; }

bool is_number(const std::string & token) {
	static const std::regex re{R""(^[-+]?[0-9]*\.?[0-9]+([eE][-+]?[0-9]+)?$)""};
	return std::regex_match(token, re);
}

bool is_time_until_s(const std::string & token) {
	static const std::regex re{"([0-9]{2}):([0-9]{2}):([0-9]{2})"};
	return std::regex_match(token, re);
}

bool is_time_until_ms(const std::string & token) {
	static const std::regex re{"([0-9]{2}):([0-9]{2}):([0-9]{2}).([0-9]{3})"};
	return std::regex_match(token, re);
}

bool is_time_until_us(const std::string & token) {
	static const std::regex re{"([0-9]{2}):([0-9]{2}):([0-9]{2}).([0-9]{6})"};
	return std::regex_match(token, re);
}

bool is_time_until_ns(const std::string & token) {
	static const std::regex re{"([0-9]{2}):([0-9]{2}):([0-9]{2}).([0-9]{9})"};
	return std::regex_match(token, re);
}

bool is_time(const std::string & token) {
	return (is_time_until_s(token) || is_time_until_ms(token) || is_time_until_us(token) || is_time_until_ns(token));
}

bool is_date_with_dash(const std::string & token) {
	static const std::regex re{R"([0-9]{4}-[0-9]{2}-[0-9]{2})"};
	return std::regex_match(token, re);
}

bool is_date_with_bar(const std::string & token) {
	static const std::regex re{R"([0-9]{4}/[0-9]{2}/[0-9]{2})"};
	return std::regex_match(token, re);
}

bool is_date(const std::string & token) {
	return (is_date_with_bar(token) || is_date_with_dash(token));
}

bool is_timestamp_with_dash(const std::string & token) {
	static const std::regex re("([0-9]{4})-([0-9]{2})-([0-9]{2})?[ T]?([0-9]{2}):([0-9]{2}):([0-9]{2})?[Z]?");
	bool ret = std::regex_match(token, re);
	return ret;
}

bool is_timestamp_with_bar(const std::string & token) {
	static const std::regex re("([0-9]{4})/([0-9]{2})/([0-9]{2})?[ T]?([0-9]{2}):([0-9]{2}):([0-9]{2})?[Z]?");
	bool ret = std::regex_match(token, re);
	return ret;
}

bool is_timestamp(const std::string & token) {
	return (is_timestamp_with_bar(token) || is_timestamp_with_dash(token));
}

bool is_timestamp_ms_with_dash(const std::string & token) {
	static const std::regex re("([0-9]{4})-([0-9]{2})-([0-9]{2})?[ T]?([0-9]{2}):([0-9]{2}):([0-9]{2}).([0-9]{3})?[Z]?");
	bool ret = std::regex_match(token, re);
	return ret;
}

bool is_timestamp_ms_with_bar(const std::string & token) {
	static const std::regex re("([0-9]{4})/([0-9]{2})/([0-9]{2})?[ T]?([0-9]{2}):([0-9]{2}):([0-9]{2}).([0-9]{3})?[Z]?");
	bool ret = std::regex_match(token, re);
	return ret;
}

bool is_timestamp_ms(const std::string & token) {
	return (is_timestamp_ms_with_bar(token) || is_timestamp_ms_with_dash(token));
}

bool is_timestamp_us_with_dash(const std::string & token) {
	static const std::regex re("([0-9]{4})-([0-9]{2})-([0-9]{2})?[ T]?([0-9]{2}):([0-9]{2}):([0-9]{2}).([0-9]{6})?[Z]?");
	bool ret = std::regex_match(token, re);
	return ret;
}

bool is_timestamp_us_with_bar(const std::string & token) {
	static const std::regex re("([0-9]{4})/([0-9]{2})/([0-9]{2})?[ T]?([0-9]{2}):([0-9]{2}):([0-9]{2}).([0-9]{6})?[Z]?");
	bool ret = std::regex_match(token, re);
	return ret;
}

bool is_timestamp_us(const std::string & token) {
	return (is_timestamp_us_with_bar(token) || is_timestamp_us_with_dash(token));
}

bool is_timestamp_ns_with_dash(const std::string & token) {
	static const std::regex re("([0-9]{4})-([0-9]{2})-([0-9]{2})?[ T]?([0-9]{2}):([0-9]{2}):([0-9]{2}).([0-9]{9})?[Z]?");
	bool ret = std::regex_match(token, re);
	return ret;
}

bool is_timestamp_ns_with_bar(const std::string & token) {
	static const std::regex re("([0-9]{4})/([0-9]{2})/([0-9]{2})?[ T]?([0-9]{2}):([0-9]{2}):([0-9]{2}).([0-9]{9})?[Z]?");
	bool ret = std::regex_match(token, re);
	return ret;
}

bool is_timestamp_ns(const std::string & token) {
	return (is_timestamp_ns_with_bar(token) || is_timestamp_ns_with_dash(token));
}

bool is_timestamp_with_decimals(const std::string & token) {
	return (is_timestamp_ms(token) || is_timestamp_us(token) || is_timestamp_ns(token));
}

bool is_timestamp_with_decimals_and_dash(const std::string & token) {
	return (is_timestamp_ns_with_dash(token) || is_timestamp_us_with_dash(token) || is_timestamp_ms_with_dash(token));
}

bool is_timestamp_with_decimals_and_bar(const std::string & token) {
	return (is_timestamp_ns_with_bar(token) || is_timestamp_us_with_bar(token) || is_timestamp_ms_with_bar(token));
}

bool is_bool(const std::string & token) { return (token == "true" || token == "false"); }

bool is_join_expression(const std::string & token) {
	static const std::regex re("=[/(][/$][0-9]{1,4},[ ][/$][0-9]{1,4}[/)]");
	bool ret = std::regex_match(token, re);
	return ret;
}

bool is_SQL_data_type(const std::string & token) {
	static std::vector<std::string> CALCITE_DATA_TYPES = {
		"TINYINT", "SMALLINT", "INTEGER", "BIGINT", "FLOAT", "DOUBLE", "DATE", "TIMESTAMP", "VARCHAR"
	};

	return std::find(std::begin(CALCITE_DATA_TYPES), std::end(CALCITE_DATA_TYPES), token) != std::end(CALCITE_DATA_TYPES);
}

bool is_operator_token(const std::string & token) {
	// can't use is_unary_operator_token(token) || is_binary_operator_token(token);
	// need to work with calcite operators too i.e. CASE, CAST, etc
	return !is_literal(token) && !is_var_column(token) && !is_SQL_data_type(token);
}

bool is_literal(const std::string & token) {
	return is_null(token) || is_bool(token) || is_number(token) || is_date(token) || is_string(token) ||
		   is_timestamp(token) || is_time(token);
}

bool is_var_column(const std::string& token){
	return token[0] == '$';
}

bool is_inequality(const std::string& token){
	return token == "<" || token == "<=" || token == ">" || token == ">=" || token == "<>";
}

std::vector<std::string> fix_column_aliases(const std::vector<std::string> & column_names, std::string expression){
	std::string aliases_string = get_named_expression(expression, "aliases");
	std::vector<std::string> aliases_string_split =
		get_expressions_from_expression_list(aliases_string, true);

	std::vector<std::string> col_names = column_names;

	// When Window Function exists more aliases are considered
	// so we just want to return the same column_names
	if (aliases_string_split.size() > col_names.size()) {
		return col_names;
	}

	// Setting the aliases only when is not an empty set
	for(size_t col_idx = 0; col_idx < aliases_string_split.size(); col_idx++) {
		// TODO: Rommel, this check is needed when for example the scan has not projects but there are extra
		// aliases
		if(col_idx < column_names.size()) {
			col_names[col_idx] = aliases_string_split[col_idx];
		}
	}

	return col_names;
}

std::string get_named_expression(const std::string & query_part, const std::string & expression_name) {
	if(query_part.find(expression_name + "=[") == query_part.npos) {
		return "";  // expression not found
	}
	int start_position = (query_part.find(expression_name + "=[[")) + 3 + expression_name.length();
	if(query_part.find(expression_name + "=[[") == query_part.npos) {
		start_position = (query_part.find(expression_name + "=[")) + 2 + expression_name.length();
	}
	int end_position = (query_part.find("]", start_position));
	return query_part.substr(start_position, end_position - start_position);
}

std::vector<int> get_projections(const std::string & query_part) {

	// On Calcite, the select count(*) case is represented with
	// the projection list empty and the aliases list containing
	// only one element as follows:
	//
	// ...
	//   BindableTableScan(table=[[main, big_taxi]], projects=[[]], aliases=[[$f0]])
	//
	// So, in such a scenario, we will load only the first column.

	if (query_part.find(" projects=[[]]") != std::string::npos){
		std::string aliases_string = get_named_expression(query_part, "aliases");
		std::vector<std::string> aliases_string_split =
			get_expressions_from_expression_list(aliases_string, true);

		std::vector<int> projections;
		if(aliases_string_split.size() == 1) {
			projections.push_back(0);
		}
		return projections;
	}

	std::string project_string = get_named_expression(query_part, "projects");
	std::vector<std::string> project_string_split =
		get_expressions_from_expression_list(project_string, true);

	std::vector<int> projections;
	for(size_t i = 0; i < project_string_split.size(); i++) {
		projections.push_back(std::stoi(project_string_split[i]));
	}
	return projections;
}

bool is_union(std::string query_part) { return (query_part.find(LOGICAL_UNION_TEXT) != std::string::npos); }

bool is_project(std::string query_part) { return (query_part.find(LOGICAL_PROJECT_TEXT) != std::string::npos); }

bool is_logical_scan(std::string query_part) { return (query_part.find(LOGICAL_SCAN_TEXT) != std::string::npos); }

bool is_bindable_scan(std::string query_part) { return (query_part.find(BINDABLE_SCAN_TEXT) != std::string::npos); }

bool is_filtered_bindable_scan(std::string query_part) {
	return is_bindable_scan(query_part) && (query_part.find("filters") != std::string::npos);
}

bool is_scan(std::string query_part) { return is_logical_scan(query_part) || is_bindable_scan(query_part); }

bool is_filter(std::string query_part) { return (query_part.find(LOGICAL_FILTER_TEXT) != std::string::npos); }

bool is_limit(std::string query_part) { return (query_part.find(LOGICAL_LIMIT_TEXT) != std::string::npos); }

bool is_sort(std::string query_part) { return (query_part.find(LOGICAL_SORT_TEXT) != std::string::npos); }

bool is_merge(std::string query_part) { return (query_part.find(LOGICAL_MERGE_TEXT) != std::string::npos); }

bool is_partition(std::string query_part) { return (query_part.find(LOGICAL_PARTITION_TEXT) != std::string::npos); }

bool is_sort_and_sample(std::string query_part) { return (query_part.find(LOGICAL_SORT_AND_SAMPLE_TEXT) != std::string::npos); }

bool is_single_node_partition(std::string query_part) { return (query_part.find(LOGICAL_SINGLE_NODE_PARTITION_TEXT) != std::string::npos); }

bool is_join(const std::string & query) { return (query.find(LOGICAL_JOIN_TEXT) != std::string::npos); }

bool is_pairwise_join(const std::string & query) { return (query.find(LOGICAL_PARTWISE_JOIN_TEXT) != std::string::npos); }

bool is_join_partition(const std::string & query) { return (query.find(LOGICAL_JOIN_PARTITION_TEXT) != std::string::npos); }

bool is_aggregate(std::string query_part) { return (query_part.find(LOGICAL_AGGREGATE_TEXT) != std::string::npos); }

bool is_compute_aggregate(std::string query_part) { return (query_part.find(LOGICAL_COMPUTE_AGGREGATE_TEXT) != std::string::npos); }

bool is_distribute_aggregate(std::string query_part) { return (query_part.find(LOGICAL_DISTRIBUTE_AGGREGATE_TEXT) != std::string::npos); }

bool is_merge_aggregate(std::string query_part) { return (query_part.find(LOGICAL_MERGE_AGGREGATE_TEXT) != std::string::npos); }

bool is_window_function(std::string query_part) { return (query_part.find("OVER") != std::string::npos); }

bool is_generate_overlaps(std::string query_part) { return (query_part.find(LOGICAL_GENERATE_OVERLAPS_TEXT) != std::string::npos); }

bool is_accumulate_overlaps(std::string query_part) { return (query_part.find(LOGICAL_ACCUMULATE_OVERLAPS_TEXT) != std::string::npos); }

bool is_window_compute(std::string query_part) { return (query_part.find(LOGICAL_COMPUTE_WINDOW_TEXT) != std::string::npos); }

bool window_expression_contains_partition_by(std::string query_part) { return (query_part.find("PARTITION") != std::string::npos); }

bool window_expression_contains_order_by(std::string query_part) { return (query_part.find("ORDER BY") != std::string::npos); }

bool window_expression_contains_bounds(std::string query_part) { return (query_part.find("BETWEEN") != std::string::npos); }

bool window_expression_contains_bounds_by_range(std::string query_part) { return (query_part.find("RANGE") != std::string::npos); }

bool is_lag_or_lead_aggregation(std::string expression) {
	return (expression == "LAG" || expression == "LEAD");
}

bool is_first_value_window(std::string expression) {
	return (expression == "FIRST_VALUE");
}

bool is_last_value_window(std::string expression) {
	return (expression == "LAST_VALUE");
}

// input: LogicalProject(min_keys=[MIN($0) OVER (PARTITION BY $2 ORDER BY $1)],
//                       max_keys=[MAX($0) OVER (PARTITION BY $2 ORDER BY $0)])
// output: true
bool window_expression_contains_multiple_diff_over_clauses(std::string logical_plan) {
	size_t num_over_clauses = StringUtil::findAndCountAllMatches(logical_plan, "OVER");
	if (num_over_clauses < 2) return false;

	std::string query_part = get_query_part(logical_plan);
	std::vector<std::string> project_expressions = get_expressions_from_expression_list(query_part);

	std::vector<std::string> full_over_expressions;
	for (size_t i = 0; i < project_expressions.size(); ++i) {
		if (is_window_function(project_expressions[i])) {
			std::string over_express = get_over_expression(project_expressions[i]);
			full_over_expressions.push_back(over_express);
		}
	}

	std::string same_over_clause = full_over_expressions[0];
	for (size_t i = 1; i < full_over_expressions.size(); ++i) {
		if (same_over_clause.compare(full_over_expressions[i]) != 0) {
			return true;
		}
	}

	return false;
}

// Due to `sum` window aggregation the OVER clause is repeated two times from Calcite
// something like:
// CASE(>(COUNT($0) OVER (PARTITION BY $1), 0), $SUM0($0) OVER (PARTITION BY $1), null:INTEGER)
bool is_sum_window_function(std::string expression) {
	size_t num_over_clauses = StringUtil::findAndCountAllMatches(expression, "OVER");
	if (num_over_clauses == 2) return true;
	
	return false;
}

// Due to `avg` window aggregation the OVER clause is repeated three times from Calcite
// something like:
// CAST(/(CASE(>(COUNT($0) OVER (PARTITION BY $1), 0), $SUM0($0) OVER (PARTITION BY $1), null:INTEGER), COUNT($0) OVER (PARTITION BY $1))):INTEGER
bool is_avg_window_function(std::string expression) {
	size_t num_over_clauses = StringUtil::findAndCountAllMatches(expression, "OVER");
	if (num_over_clauses == 3) return true;
	
	return false;
}

// input: max_prices=[MAX($0) OVER (PARTITION BY $2 ORDER BY $0, $1)]
// output: max_prices=[MAX($0)]
std::string remove_over_expr(std::string expression) {

	std::string over_expression = get_over_expression(expression);
	std::string expression_to_remove = " OVER (" + over_expression + ")";
	std::string removed_expression = StringUtil::replace(expression, expression_to_remove, "");

	return removed_expression;
}

// Will replace the `COUNT($X)` expression with the right window col index
// useful when exists an `avg` or `sum` window expression
// input: CASE(>(COUNT($0), 0), $SUM0($0), null:INTEGER)
// output: CASE(>($4, 0), $SUM0($0), null:INTEGER)
std::string replace_count_expr_with_right_index(std::string expression, size_t rigt_index) {
	std::string removed_expression, express_to_remove;
	size_t start_index = expression.find("COUNT");
	// When a CAST was applied to a projected column
	if (expression.find("COUNT(CAST") != expression.npos) {
		removed_expression = StringUtil::replace(expression, "COUNT(", "");
		size_t end_cast_index = removed_expression.find(", 0)");
		removed_expression.erase(removed_expression.begin() + end_cast_index - 1);
		removed_expression = removed_expression.substr(0, removed_expression.size() - 1);

		size_t new_start_index = removed_expression.find("$");
		size_t new_end_index = removed_expression.find(")");
		std::string old_index = removed_expression.substr(new_start_index + 1, new_end_index - new_start_index - 1);
		removed_expression = StringUtil::replace(removed_expression, "$" + old_index, "$" + std::to_string(rigt_index));
	} else {
		size_t end_index = expression.find(")");
		express_to_remove = expression.substr(start_index, end_index - start_index + 1);
		removed_expression = StringUtil::replace(expression, express_to_remove, "$" + std::to_string(rigt_index));
	}

	return removed_expression;
}

// Will replace the `$SUM0($X)` expression with the right window col index
// useful when exists an `avg` or `sum` window expression
// input: CASE(>($4, 0), $SUM0($0), null:INTEGER)
// output: CASE(>($4, 0), $5, null:INTEGER)
std::string replace_sum0_expr_with_right_index(std::string expression, size_t rigt_index) {

	std::string removed_expression;
	size_t start_index = expression.find("$SUM0");
	size_t end_index = expression.find("null:") - 2;

	if (expression.find("$SUM0(CAST") != expression.npos) {
		removed_expression = StringUtil::replace(expression, "$SUM0(", "");
		size_t end_index_2 = removed_expression.find(", null:");
		removed_expression.erase(removed_expression.begin() + end_index_2 - 1);
		removed_expression = StringUtil::replace(removed_expression, "0), CAST($" + std::to_string(rigt_index), "0), CAST($" + std::to_string(rigt_index + 1));
	} else {
		std::string express_to_remove = expression.substr(start_index, end_index - start_index);
		removed_expression = StringUtil::replace(expression, express_to_remove, "$" + std::to_string(rigt_index + 1));
	}

	return removed_expression;
}

// input: LogicalProject(sum_max_prices=[$0], o_orderkey=[$1], o_min_prices=[$2]])
// output: sum_max_prices=[$0], o_orderkey=[$1], o_min_prices=[$2]]
std::string get_query_part(std::string logical_plan) {
	std::string query_part = logical_plan.substr(
		logical_plan.find("(") + 1, logical_plan.rfind(")") - logical_plan.find("(") - 1);

	return query_part;
}

// input: min_val=[MIN($0) OVER (PARTITION BY $2 ORDER BY $1 ROWS BETWEEN 4 PRECEDING AND 3 FOLLOWING)]
// output: < 4, 3 >
std::tuple< int, int > get_bounds_from_window_expression(const std::string & logical_plan) {
	int preceding_value, following_value;

	std::string over_clause = get_first_over_expression_from_logical_plan(logical_plan, "PARTITION BY");
	if (over_clause.length() == 0){
		over_clause = get_first_over_expression_from_logical_plan(logical_plan, "ORDER BY");
	}

	// the default behavior when not bounds are passed is
	// RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW. 
	if (over_clause.find("BETWEEN") == std::string::npos) {
		preceding_value = -1;
		following_value = 0;
		return std::make_tuple(preceding_value, following_value);
	}

	// getting the first limit value
	std::string between_expr = "BETWEEN ";
	std::string and_expr = "AND ";
	size_t start_pos = over_clause.find(between_expr) + between_expr.size();
	size_t end_pos = over_clause.find(and_expr);

	std::string preceding_clause = over_clause.substr(start_pos, end_pos - start_pos);
	std::string following_clause = over_clause.substr(end_pos + and_expr.size());

	if (preceding_clause.find("CURRENT ROW") != std::string::npos){
		preceding_value = 0;
	} else if (preceding_clause.find("UNBOUNDED") != std::string::npos){
		preceding_value = -1;
	} else {
		std::string preceding_expr = " PRECEDING";
		end_pos = preceding_clause.find(preceding_expr);
		std::string str_value = preceding_clause.substr(0, end_pos);
		preceding_value = std::stoi(str_value);
	}

	// getting the second limit value
	if (following_clause.find("CURRENT ROW") != std::string::npos){
		following_value = 0;
	} else if (following_clause.find("UNBOUNDED") != std::string::npos){
		following_value = -1;
	} else {
		std::string following_expr = " FOLLOWING";
		end_pos = following_clause.find(following_expr);
		std::string str_value = following_clause.substr(0, end_pos);
		following_value = std::stoi(str_value);
	}

	return std::make_tuple(preceding_value, following_value);
}

std::string get_frame_type_from_over_clause(const std::string & logical_plan) {
	std::string query_part = get_query_part(logical_plan);
	if (is_window_function(query_part) && query_part.find("ROWS") != query_part.npos) {
		return "ROWS";
	}

	return "RANGE";
}

// input: min_keys=[MIN($0) OVER (PARTITION BY $1, $2 ORDER BY $0)]
// output: PARTITION BY $1, $2 ORDER BY $0
std::string get_over_expression(std::string query_part) {
	std::string expression_name = "OVER (";
	size_t pos = query_part.find(expression_name);

	if (pos == std::string::npos) {
		return "";
	}

	std::string reduced_query_part = query_part.substr(pos + expression_name.size(), query_part.size());

	// Sometimes there are more than one OVER clause, for instance: SUM and AVG
	if (reduced_query_part.find(expression_name) != std::string::npos) {
		reduced_query_part = get_over_expression(reduced_query_part);
	}

	return reduced_query_part.substr(0, reduced_query_part.find(")"));
}

// input: LogicalProject(min_keys=[MIN($0) OVER (PARTITION BY $2 ORDER BY $1)], max_keys=[MAX($0) OVER (PARTITION BY $2)])
// output: PARTITION BY $2 ORDER BY $1
std::string get_first_over_expression_from_logical_plan(const std::string & logical_plan, const std::string & expr) {
	std::string over_expression;

	std::string query_part = get_query_part(logical_plan);
	if (query_part.find(expr) == query_part.npos) {
		return over_expression;
	}
	// at least there is one PARTITION BY
	std::vector<std::string> project_expressions = get_expressions_from_expression_list(query_part);
	size_t first_pos_with_over;

	// TODO: for now all the OVER clauses MUST be the same
	for (size_t i = 0; i < project_expressions.size(); ++i) {
		if (project_expressions[i].find(expr) != std::string::npos) {
			first_pos_with_over = i;
			break;
		}
	}

	over_expression = get_over_expression(project_expressions[first_pos_with_over]);

	return over_expression;
}

// input: LogicalComputeWindow(min_keys=[MIN($0) OVER (PARTITION BY $2 ORDER BY $1)],
//                             max_keys=[MAX(4) OVER (PARTITION BY $2 ORDER BY $1)],
//                             lead_val=[LEAD($0, 3) OVER (PARTITION BY $2 ORDER BY $1)])
// output: < [0, 4, 0], ["MIN", "MAX", "LEAD"], [0, 0, 3] >
std::tuple< std::vector<int>, std::vector<std::string>, std::vector<int> > 
get_cols_to_apply_window_and_cols_to_apply_agg(const std::string & logical_plan) {
	std::vector<int> column_index;
	std::vector<std::string> aggregations;
	std::vector<int> agg_param_values; // will be set to 0 when not actually used

	std::string query_part = get_query_part(logical_plan);
	std::vector<std::string> project_expressions = get_expressions_from_expression_list(query_part);

	// we want all expressions that contains an OVER clause
	for (size_t i = 0; i < project_expressions.size(); ++i) {
 		if (project_expressions[i].find("OVER") != std::string::npos) {
 			std::string express_i = project_expressions[i];
 			size_t start_pos = express_i.find("[") + 1;
 			size_t end_pos = express_i.find("OVER");
 			express_i = express_i.substr(start_pos, end_pos - start_pos);
 			std::string express_i_wo_trim = StringUtil::trim(express_i);
 			std::vector<std::string> split_parts = StringUtil::split(express_i_wo_trim, "($");
 			if (split_parts[0] == "ROW_NUMBER()") {
 				aggregations.push_back(StringUtil::replace(split_parts[0], "()", ""));
 				column_index.push_back(0);
				agg_param_values.push_back(0);
 			} else if (split_parts[0] == "LAG" || split_parts[0] == "LEAD") {
 				// we need to get the constant values
 				std::string right_express = StringUtil::replace(split_parts[1], ")", "");
 				std::vector<std::string> inside_parts = StringUtil::split(right_express, ", ");
 				aggregations.push_back(split_parts[0]);
 				column_index.push_back(std::stoi(inside_parts[0]));
 				agg_param_values.push_back(std::stoi(inside_parts[1]));
 			} else if ( is_sum_window_function(project_expressions[i]) || is_avg_window_function(project_expressions[i]) ) {
 				aggregations.push_back("COUNT");
 				aggregations.push_back("$SUM0");
 				std::string indice = split_parts[1].substr(0, split_parts[1].find(")"));
 				column_index.push_back(std::stoi(indice));
 				column_index.push_back(std::stoi(indice));
				agg_param_values.push_back(0);
				agg_param_values.push_back(0);
			} else if (is_last_value_window(project_expressions[i])) {
				aggregations.push_back(split_parts[0]);
				std::string col_index = StringUtil::replace(split_parts[1], ")", "");
				column_index.push_back(std::stoi(col_index));
				agg_param_values.push_back(-1);			
 			} else {
 				aggregations.push_back(split_parts[0]);
 				std::string col_index = StringUtil::replace(split_parts[1], ")", "");
 				column_index.push_back(std::stoi(col_index));
				agg_param_values.push_back(0);
 			}
 		}
 	}

	return std::make_tuple(column_index, aggregations, agg_param_values);
}

// This function will update all the expressions that contains a window function
// inputs:
// expressions: [ MIN($0) OVER (PARTITION BY $2 ORDER BY $1), MAX($0) OVER (PARTITION BY $2 ORDER BY $1), $0, $2, $3 ]
// num_columns: 6
// output: [ $4, $5, $0, $2, $3]
std::vector<std::string> clean_window_function_expressions(const std::vector<std::string> & expressions, size_t num_columns) {
	// First of all, we need to know how many window columns exists
    size_t total_wf_cols = 0;
    std::vector<std::string> new_expressions = expressions;
    for(size_t col_i = 0; col_i < new_expressions.size(); col_i++) {
        if (is_window_function(new_expressions[col_i])) {
            if (is_sum_window_function(new_expressions[col_i]) || is_avg_window_function(new_expressions[col_i])) {
				// due to `sum` or `avg` window aggregation two columns were added (in ComputeWindowKernel)
                total_wf_cols += 2;
            }
            else total_wf_cols++;
        }
    }

    // Now let's update the expressions which contains Window functions
    if (total_wf_cols > 0) {
        size_t wf_count = 0;
        for(size_t col_i = 0; col_i < new_expressions.size(); col_i++) {
            size_t rigt_index = num_columns - total_wf_cols + wf_count;
            if (wf_count < total_wf_cols && is_window_function(new_expressions[col_i])) {
                if ( is_sum_window_function(new_expressions[col_i]) || is_avg_window_function(new_expressions[col_i]) ){
                    std::string removed_expression = remove_over_expr(new_expressions[col_i]);
                    removed_expression = replace_count_expr_with_right_index(removed_expression, rigt_index);
                    new_expressions[col_i] = replace_sum0_expr_with_right_index(removed_expression, rigt_index);
					// due to `sum` or `avg` window aggregation two columns were added (in ComputeWindowKernel)
                    wf_count += 2;
                } else {
                    new_expressions[col_i] = "$" + std::to_string(rigt_index);
                    wf_count++;
                }
            }
        }
    }

	return new_expressions;
}

// Returns the index from table_scan if exists
size_t get_table_index(std::vector<std::string> table_scans, std::string table_scan) {

	for (size_t i = 0; i < table_scans.size(); i++){
		if (StringUtil::contains(table_scans[i], table_scan)){
			return i;
		}
	}
	throw std::invalid_argument("ERROR: get_table_index table_scan was not found ==>" + table_scan);
}

// Input: [[hr, emps]] or [[emps]] Output: hr.emps or emps
std::string extract_table_name(std::string query_part) {
	size_t start = query_part.find("[[") + 2;
	size_t end = query_part.find("]]");
	std::string table_name_text = query_part.substr(start, end - start);
	std::vector<std::string> table_parts = StringUtil::split(table_name_text, ',');
	std::string table_name = "";
	for(size_t i = 0; i < table_parts.size(); i++) {
		if(table_parts[i][0] == ' ') {
			table_parts[i] = table_parts[i].substr(1, table_parts[i].size() - 1);
		}
		table_name += table_parts[i];
		if(i != table_parts.size() - 1) {
			table_name += ".";
		}
	}

	return table_name;
}


// takes a comma delimited list of expressions and splits it into separate expressions
std::vector<std::string> get_expressions_from_expression_list(std::string & combined_expression, bool trim) {
	combined_expression = replace_calcite_regex(combined_expression);

	std::vector<std::string> expressions;

    size_t curInd = 0;
    size_t curStart = 0;
	bool inQuotes = false;
	int parenthesisDepth = 0;
	int sqBraketsDepth = 0;
	while(curInd < combined_expression.size()) {
		if(inQuotes) {
			if(combined_expression[curInd] == '\'') {
				if(!(curInd + 1 < combined_expression.size() &&
					   combined_expression[curInd + 1] ==
						   '\'')) {  // if we are in quotes and we get a double single quotes, that is an escaped quotes
					inQuotes = false;
				}
			}
		} else {
			if(combined_expression[curInd] == '\'') {
				inQuotes = true;
			} else if(combined_expression[curInd] == '(') {
				parenthesisDepth++;
			} else if(combined_expression[curInd] == ')') {
				parenthesisDepth--;
			} else if(combined_expression[curInd] == '[') {
				sqBraketsDepth++;
			} else if(combined_expression[curInd] == ']') {
				sqBraketsDepth--;
			} else if(combined_expression[curInd] == ',' && parenthesisDepth == 0 && sqBraketsDepth == 0) {
				std::string exp = combined_expression.substr(curStart, curInd - curStart);

				if(trim)
					expressions.push_back(StringUtil::ltrim(exp));
				else
					expressions.push_back(exp);

				curStart = curInd + 1;
			}
		}
		curInd++;
	}

	if(curStart < combined_expression.size() && curInd <= combined_expression.size()) {
		std::string exp = combined_expression.substr(curStart, curInd - curStart);

		if(trim)
			expressions.push_back(StringUtil::trim(exp));
		else
			expressions.push_back(exp);
	}

	return expressions;
}

std::string replace_calcite_regex(const std::string & expression) {
	std::string ret = expression;

	static const std::regex count_re{R""(COUNT\(DISTINCT (\W\(.+?\)|.+)\))"", std::regex_constants::icase};
	ret = std::regex_replace(ret, count_re, "COUNT_DISTINCT($1)");

	static const std::regex char_collate_re{
		R""((?:\(\d+\))? CHARACTER SET ".+?" COLLATE ".+?")"", std::regex_constants::icase};
	ret = std::regex_replace(ret, char_collate_re, "");

	static const std::regex timestamp_re{R""(TIMESTAMP\(\d+\))"", std::regex_constants::icase};
	ret = std::regex_replace(ret, timestamp_re, "TIMESTAMP");

	static const std::regex decimal_re{
		R""(DECIMAL\(\d+, \d+\))"", std::regex_constants::icase};
	ret = std::regex_replace(ret, decimal_re, "DOUBLE");

	static const std::regex char_re{
		R""(:CHAR\(\d+\))"", std::regex_constants::icase};
	ret = std::regex_replace(ret, char_re, ":VARCHAR");


	StringUtil::findAndReplaceAll(ret, "IS NOT DISTINCT FROM", "IS_NOT_DISTINCT_FROM");
	StringUtil::findAndReplaceAll(ret, "IS NOT NULL", "IS_NOT_NULL");
	StringUtil::findAndReplaceAll(ret, "IS NULL", "IS_NULL");
	StringUtil::findAndReplaceAll(ret, " NOT NULL", "");
	StringUtil::findAndReplaceAll(ret, "RAND()", "BLZ_RND()");
	StringUtil::findAndReplaceAll(ret, "EXTRACT(FLAG(YEAR), ", "BL_YEAR(");
	StringUtil::findAndReplaceAll(ret, "EXTRACT(FLAG(MONTH), ", "BL_MONTH(");
	StringUtil::findAndReplaceAll(ret, "EXTRACT(FLAG(DAY), ", "BL_DAY(");
	StringUtil::findAndReplaceAll(ret, "EXTRACT(FLAG(DOW), ", "BL_DOW(");
	StringUtil::findAndReplaceAll(ret, "EXTRACT(FLAG(HOUR), ", "BL_HOUR(");
	StringUtil::findAndReplaceAll(ret, "EXTRACT(FLAG(MINUTE), ", "BL_MINUTE(");
	StringUtil::findAndReplaceAll(ret, "EXTRACT(FLAG(SECOND), ", "BL_SECOND(");
	// Flag options for TRIM
	StringUtil::findAndReplaceAll(ret, "TRIM(FLAG(BOTH),", "TRIM(\"BOTH\",");
	StringUtil::findAndReplaceAll(ret, "TRIM(FLAG(LEADING),", "TRIM(\"LEADING\",");
	StringUtil::findAndReplaceAll(ret, "TRIM(FLAG(TRAILING),", "TRIM(\"TRAILING\",");
	StringUtil::findAndReplaceAll(ret, "DAY TO SECOND", "SECOND");
	StringUtil::findAndReplaceAll(ret, "HOUR TO SECOND", "SECOND");
	StringUtil::findAndReplaceAll(ret, "MINUTE TO SECOND", "SECOND");

	StringUtil::findAndReplaceAll(ret, "/INT(", "/(");

	return ret;
}

std::tuple< bool, bool, std::vector<std::string> > bypassingProject(std::string logical_plan, std::vector<std::string> names) {
	bool by_passing_project = false, by_passing_project_with_aliases = false;
	std::vector<std::string> aliases;

	std::string combined_expression = get_query_part(logical_plan);
	std::vector<std::string> named_expressions = get_expressions_from_expression_list(combined_expression);
	std::vector<std::string> expressions(named_expressions.size());
	aliases.resize(named_expressions.size());

	for(size_t i = 0; i < named_expressions.size(); ++i) {
		const std::string & named_expr = named_expressions[i];
		aliases[i] = named_expr.substr(0, named_expr.find("=["));
		expressions[i] = named_expr.substr(named_expr.find("=[") + 2 , (named_expr.size() - named_expr.find("=[")) - 3);
	}

	if (names.size() != aliases.size()) {
		return std::make_tuple(false, false, aliases);
	}
	
	for(size_t i = 0; i < expressions.size(); ++i) {
		if (expressions[i] != "$" + std::to_string(i)) {
			return std::make_tuple(false, false, aliases);
		}
	}

	by_passing_project = true;

	// At this step, expressions: [$0, $1, $2, $3], let's see the aliases
	for(size_t i = 0; i < aliases.size(); ++i) {
		if (aliases[i] != names[i]) {
			by_passing_project_with_aliases = true;
			break;
		}
	}

	return std::make_tuple(by_passing_project, by_passing_project_with_aliases, aliases);
}

// input: -($3)
// output: -(0, $3)
std::string fill_minus_op_with_zero(std::string expression) {
	std::size_t total_vars = StringUtil::findAndCountAllMatches(expression, "$");
	if (expression.at(0) == '-' && total_vars == 1 && expression.find(",") == expression.npos) {
		std::string left_expr = expression.substr(0, 2);
		std::string right_expr = expression.substr(2, expression.size() - left_expr.size());
		expression = left_expr + "0, " + right_expr;
	}

	return expression;
}

// input: CONCAT($0, ' - ', CAST($1):VARCHAR, ' : ', $2)
// output: "CONCAT(CONCAT(CONCAT(CONCAT($0, ' - '), CAST($1):VARCHAR), ' : '), $2)"
std::string convert_concat_expression_into_multiple_binary_concat_ops(std::string expression) {
	size_t start_concat_pos = expression.find("CONCAT(");
	if (start_concat_pos == expression.npos) {
		return expression;
	}

	std::string concat_expression;
	bool start_with_concat = false;
	if (start_concat_pos != 0) {
		// Let's find out how many `(` there are before `CONCAT`
		std::string left_expression = expression.substr(0, start_concat_pos);
		size_t total_open_parenth = StringUtil::findAndCountAllMatches(left_expression, "(");

		std::string reverse_expression(expression);
		std::reverse(reverse_expression.begin(), reverse_expression.end());

		size_t offset = 0;
		for (size_t i = 0; i <= total_open_parenth; ++i) {
			size_t closed_parenth_pos = reverse_expression.find(")");
			offset += closed_parenth_pos;
			reverse_expression = reverse_expression.substr(closed_parenth_pos + 1, expression.size() - (closed_parenth_pos + 1));
		}
		concat_expression = expression.substr(start_concat_pos, expression.size() - offset - start_concat_pos - 1);
	} else {
		start_with_concat = true;
		concat_expression = expression;
	}

	// just to remove `CONCAT( )`
	std::string expression_wo_concat = get_query_part(concat_expression);
	std::vector<std::string> expressions_to_concat = get_expressions_from_expression_list(expression_wo_concat);

	if (expressions_to_concat.size() < 2) throw std::runtime_error("CONCAT operator must have at least two children, as CONCAT($0, $1) .");

	std::string new_expression =  "CONCAT(" + expressions_to_concat[0] + ", " + expressions_to_concat[1] + ")";

	for (size_t i = 2; i < expressions_to_concat.size(); ++i) {
		new_expression = "CONCAT(" + new_expression + ", " + expressions_to_concat[i] + ")";
	}

	std::string output_expression;
	if (start_with_concat) {
		output_expression = new_expression;
	} else {
		output_expression = StringUtil::replace(expression, concat_expression, new_expression);
	}

	return output_expression;
}

const std::string remove_quotes_from_timestamp_literal(const std::string & scalar_string) {
	if (scalar_string[0] != '\'' && scalar_string[scalar_string.size() - 1] != '\'') {
		return scalar_string;
	}

	std::string temp_str = scalar_string;
	const std::string cleaned_timestamp = temp_str.substr(1, temp_str.size() - 2);

	return cleaned_timestamp;
}

// Calcite translates the `col_1 IS NOT DISTINCT FROM col_3` subquery to the
//     OR(AND(IS NULL($1), IS NULL($3)), IS TRUE(=($1 , $3)))  
// expression. So let's do the same here
// input: IS_NOT_DISTINCT_FROM($1, $3)
// output: OR(AND(IS NULL($1), IS NULL($3)), IS TRUE(=($1 , $3)))
std::string replace_is_not_distinct_as_calcite(std::string expression) {
	if (expression.find("IS_NOT_DISTINCT_FROM") == expression.npos) {
		return expression;
	}
	
	size_t start_pos = expression.find("(") + 1;
	size_t end_pos = expression.find(")");
	
	// $1, $3
	std::string reduced_expr = expression.substr(start_pos, end_pos - start_pos);
	std::vector<std::string> var_str = get_expressions_from_expression_list(reduced_expr);

	if (var_str.size() != 2) {
		throw std::runtime_error("IS_NOT_DISTINCT_FROM must contains only 2 variables.");
	}
    
    return "OR(AND(IS NULL(" + var_str[0] + "), IS NULL(" + var_str[1] + ")), IS TRUE(=(" + var_str[0] + " , " + var_str[1] + ")))";
}

// input: AND(=($0, $3), IS_NOT_DISTINCT_FROM($1, $3))
// output: ["=($0, $3)" , "OR(AND(IS NULL($1), IS NULL($3)), IS TRUE(=($1 , $3)))"]
std::tuple<std::string, std::string> update_join_and_filter_expressions_from_is_not_distinct_expr(const std::string & expression) {

	std::string origin_expr = expression;
	size_t total_is_not_distinct_expr = StringUtil::findAndCountAllMatches(origin_expr, "IS_NOT_DISTINCT_FROM");
	
	std::string left_expr = origin_expr.substr(0, 4);
	if (!(left_expr == "AND(") || (total_is_not_distinct_expr == 0)) {
		return std::make_tuple(origin_expr, "");
	}

	// "=($0, $3), IS_NOT_DISTINCT_FROM($1, $3)"
	std::string reduced_expr = origin_expr.erase(0, left_expr.size());
	reduced_expr = reduced_expr.substr(0, reduced_expr.size() - 1);
	std::vector<std::string> all_expressions = get_expressions_from_expression_list(reduced_expr);

	// Let's get the index of the join condition (using regex) if exists
	size_t join_pos_operation;
	bool contains_join_express = false;
	for (size_t i = 0; i < all_expressions.size(); ++i) {
		if (is_join_expression(all_expressions[i])) {
			join_pos_operation = i;
			contains_join_express = true;
			break;
		}
	}

	if (!contains_join_express) {
		return std::make_tuple(expression, "");
	}

	size_t total_filter_conditions = all_expressions.size() - 1;

	std::string new_join_statement_express = all_expressions[join_pos_operation], filter_statement_expression;

	assert(total_filter_conditions > 0);

	if (total_filter_conditions == 1) {
		size_t not_join_pos = (join_pos_operation == 0) ? 1 : 0;
		filter_statement_expression = replace_is_not_distinct_as_calcite(all_expressions[not_join_pos]);
	} else {
		filter_statement_expression = "AND(";
		for (size_t i = 0; i < all_expressions.size(); ++i) {
			if (i != join_pos_operation) {
				filter_statement_expression += replace_is_not_distinct_as_calcite(all_expressions[i]) + ", ";
			}
		}
		filter_statement_expression = filter_statement_expression.substr(0, filter_statement_expression.size() - 2);
		filter_statement_expression += ")";
	}

	return std::make_tuple(new_join_statement_express, filter_statement_expression);
}

bool is_cast_to_timestamp(std::string expression) {
	return (expression.find("CAST(") != expression.npos && expression.find(":TIMESTAMP") != expression.npos);
}

bool is_cast_to_date(std::string expression) {
	return (expression.find("CAST(") != expression.npos && expression.find(":DATE") != expression.npos);
}

// Calcite by default returns units in milliseconds, as we are getting a
// TIMESTAMP_NANOSECONDS from BLZ_TO_TIMESTAMP, we want to convert these to nanoseconds units
// input: CAST(/INT(Reinterpret(-(2020-10-15 10:58:02, CAST($0):TIMESTAMP(0))), 86400000)):INTEGER
// output: CAST(/INT(Reinterpret(-(2020-11-10 12:00:01, CAST($0):TIMESTAMP(0))), 86400000000000)):INTEGER
std::string convert_ms_to_ns_units(std::string expression) {
	if (!is_cast_to_timestamp(expression) && !is_cast_to_date(expression) ) {
		return expression;
	}

	std::string ns_str_to_concat = "000000";
	// For timestampdiff
	if (expression.find("Reinterpret(") != expression.npos) {
		std::string day_ms = "86400000", hour_ms = "3600000", min_ms = "60000", sec_ms = "1000";
		if (expression.find(day_ms) != expression.npos) {
			return StringUtil::replace(expression, day_ms, day_ms + ns_str_to_concat);
		} else if(expression.find(hour_ms) != expression.npos) {
			return StringUtil::replace(expression, hour_ms, hour_ms + ns_str_to_concat);
		} else if(expression.find(min_ms) != expression.npos) {
			return StringUtil::replace(expression, min_ms, min_ms + ns_str_to_concat);
		} else if(expression.find(sec_ms) != expression.npos) {
			return StringUtil::replace(expression, sec_ms, sec_ms + ns_str_to_concat);
		}
	}

	// For timestampdadd
	size_t start_interval_pos = expression.find(":INTERVAL");
	if (start_interval_pos != expression.npos) {
		size_t start_pos = expression.find(", ") + 2;
		std::string time_value_str = expression.substr(start_pos, start_interval_pos - start_pos);
		return StringUtil::replace(expression, time_value_str, time_value_str + ns_str_to_concat);	 
	}

	return expression;	
}

// input: CAST($0):TIMESTAMP
// output: 0
size_t get_index_from_expression_str(std::string expression) {
	// $0
	if (expression[0] == '$') {
		expression = expression.substr(1, expression.size() - 1);
		return std::stoi(expression);
	}

	// CAST($0):TIMESTAMP
	size_t start_pos = expression.find('$') + 1;
	size_t end_pos = expression.find(')');

	expression = expression.substr(start_pos, end_pos - start_pos);
	return std::stoi(expression);
}

// By default any TIMESTAMP literal expression is handled as TIMESTAMP_NANOSECONDS
// Using the `Reinterpret` clause we can get the right TIMESTAMP unit using the expression
// expression: CAST(/INT(Reinterpret(-(1996-12-01 12:00:01, $0)), 86400000)):INTEGER
std::string reinterpret_timestamp(std::string expression, std::vector<cudf::data_type> table_schema) {
	if (table_schema.size() == 0) return expression;

	std::string reint_express = "Reinterpret(-(";
	size_t start_reint_pos = expression.find(reint_express);
	if (start_reint_pos == expression.npos) {
		return expression;
	}

	size_t remove_until_pos = start_reint_pos + reint_express.size();
	std::string reduced_expr = expression.substr(remove_until_pos, expression.size() - remove_until_pos);
	size_t start_closing_parent_pos = reduced_expr.find("))");

	// 1996-12-01 12:00:01, $0
	reduced_expr = reduced_expr.substr(0, start_closing_parent_pos);

	std::vector<std::string> reduced_expressions = get_expressions_from_expression_list(reduced_expr);
	std::string left_expression = reduced_expressions[0], right_expression = reduced_expressions[1];
	std::string timest_str;
	size_t col_indice;

	assert(reduced_expressions.size() == 2);

	// Cases 1:  Reinterpret(-(1996-12-01 12:00:01, $0))
	//       2:  Reinterpret(-(1996-12-01 12:00:01, CAST($0):TIMESTAMP))
	if (is_timestamp(left_expression)) {
		timest_str = left_expression;
		col_indice = get_index_from_expression_str(right_expression);
	}
	// Cases  1:  Reinterpret(-($0, 1996-12-01 12:00:01))
	//        2:  Reinterpret(-(CAST($0):TIMESTAMP, 1996-12-01 12:00:01))
	else if (is_timestamp(right_expression)) {
		timest_str = right_expression;
		col_indice = get_index_from_expression_str(left_expression);
	} else {
		return expression;
	}

	if (table_schema[col_indice].id() == cudf::type_id::TIMESTAMP_SECONDS) {
		expression = StringUtil::replace(expression, timest_str, "CAST(" + timest_str + "):TIMESTAMP_SECONDS");
	} else if (table_schema[col_indice].id() == cudf::type_id::TIMESTAMP_MILLISECONDS) {
		expression = StringUtil::replace(expression, timest_str, "CAST(" + timest_str + "):TIMESTAMP_MILLISECONDS");
	} else if (table_schema[col_indice].id() == cudf::type_id::TIMESTAMP_MICROSECONDS) {
		expression = StringUtil::replace(expression, timest_str, "CAST(" + timest_str + "):TIMESTAMP_MICROSECONDS");
	} else {
		expression = StringUtil::replace(expression, timest_str, "CAST(" + timest_str + "):TIMESTAMP");
	}

	return expression;
}

// By default Calcite returns Interval types in ms unit. So we want to convert them to the right INTERVAL unit
// to do correct operations
// TODO: any issue related with INTERVAL operations (and parsing expressions) should be handled here
std::string apply_interval_conversion(std::string expression, std::vector<cudf::data_type> table_schema) {
	std::string interval_expr = ":INTERVAL";
	if (table_schema.size() == 0 || expression.find(interval_expr) == expression.npos) return expression;

	// op with intervals
	if (expression.find("+") != expression.npos || expression.find("-") != expression.npos) {
		std::string plus_expr = "+($", sub_expr = "-($";

		size_t start_pos = 0;
		if (expression.find(plus_expr) != expression.npos) {
			start_pos = expression.find(plus_expr) + plus_expr.size();
		} else if (expression.find(sub_expr) != expression.npos) {
			start_pos = expression.find(sub_expr) + sub_expr.size();
		}
		
		std::string new_expr = expression.substr(start_pos, expression.size() - start_pos);
		size_t last_pos = new_expr.find(", ");
		new_expr = new_expr.substr(0, last_pos);
		int col_indice =  std::stoi(new_expr);

		if (table_schema[col_indice].id() == cudf::type_id::DURATION_SECONDS) {
			return StringUtil::replace(expression, "000" + interval_expr, interval_expr);
		} else if (table_schema[col_indice].id() == cudf::type_id::DURATION_MICROSECONDS) {
			return StringUtil::replace(expression, "000" + interval_expr, "000000" + interval_expr);
		} else if (table_schema[col_indice].id() == cudf::type_id::DURATION_NANOSECONDS) {
			return StringUtil::replace(expression, "000" + interval_expr, "000000000" + interval_expr);
		} else return expression; // duration ms
	}

	// Literal interval
	if (expression.find("$") == expression.npos && expression.find("000:INTERVAL") != expression.npos) {
		return StringUtil::replace(expression, "000" + interval_expr, interval_expr);
	}

	return expression;
}

// For this function `indices` vector starts empty
// inputs:
// 	expression: "$0, $1, $3"
//  indices: []
// output: "+(+(CAST($0):INTEGER, CAST($1):INTEGER), CAST($3):INTEGER)"
std::string modify_multi_column_count_expression(std::string expression, std::vector<int> & indices) {
	size_t first_post = expression.find("$");
	if (first_post == expression.npos) return expression;

	std::string reduced_expr = expression.substr(first_post + 1, expression.size() - first_post);
	StringUtil::findAndReplaceAll(reduced_expr, "$", "");
	std::vector<std::string> indices_str = get_expressions_from_expression_list(reduced_expr);

	if (indices_str.size() == 1) {
		indices.push_back(std::stoi(indices_str[0]));
		return expression;
	}

	for (size_t i = 0; i < indices_str.size(); ++i) {
		indices.push_back(std::stoi(indices_str[i]));
	}

	std::string add_expr = "+(", cast_expr = "CAST($", integer_expr = "):INTEGER";
	expression = add_expr + cast_expr + std::to_string(indices[0]) + integer_expr + ", " + cast_expr + std::to_string(indices[1]) + integer_expr + ")";

	for (size_t i = 2; i < indices.size(); ++i) {
		expression = add_expr + expression + ", " + cast_expr + std::to_string(indices[i]) + integer_expr + ")";
	}

	return expression;
}

std::string get_current_date_or_timestamp(std::string expression, blazingdb::manager::Context * context) {
    // We want `CURRENT_TIME` holds the same value as `CURRENT_TIMESTAMP`
	if (expression.find("CURRENT_TIME") != expression.npos) {
		expression = StringUtil::replace(expression, "CURRENT_TIME", "CURRENT_TIMESTAMP");
	}

	std::size_t date_pos = expression.find("CURRENT_DATE");
	std::size_t timestamp_pos = expression.find("CURRENT_TIMESTAMP");

	if (date_pos == expression.npos && timestamp_pos == expression.npos) {
		return expression;
	}

    // CURRENT_TIMESTAMP will return a `ms` format
	std::string	timestamp_str = context->getCurrentTimestamp().substr(0, 23);
    std::string str_to_replace = "CURRENT_TIMESTAMP";

	// In case CURRENT_DATE we want only the date value
	if (date_pos != expression.npos) {
		str_to_replace = "CURRENT_DATE";
        timestamp_str = timestamp_str.substr(0, 10);
	}

	return StringUtil::replace(expression, str_to_replace, timestamp_str);
}

std::string preprocess_expression_for_evaluation(std::string expression, blazingdb::manager::Context * context, std::vector<cudf::data_type> schema) {
	expression = fill_minus_op_with_zero(expression);
	expression = convert_concat_expression_into_multiple_binary_concat_ops(expression);
	expression = get_current_date_or_timestamp(expression, context);
	expression = convert_ms_to_ns_units(expression);
	expression = reinterpret_timestamp(expression, schema);
	expression = apply_interval_conversion(expression, schema);
  return expression;
}
