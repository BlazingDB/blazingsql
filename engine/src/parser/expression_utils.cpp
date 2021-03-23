#include <map>
#include <regex>
#include <cassert>
#include <blazingdb/io/Util/StringUtil.h>

#include "expression_utils.hpp"
#include "CalciteExpressionParsing.h"
#include "error.hpp"

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
	case operator_type::BLZ_CAST_TINYINT:
	case operator_type::BLZ_CAST_SMALLINT:
	case operator_type::BLZ_CAST_INTEGER:
	case operator_type::BLZ_CAST_BIGINT:
	case operator_type::BLZ_CAST_FLOAT:
	case operator_type::BLZ_CAST_DOUBLE:
	case operator_type::BLZ_CAST_DATE:
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
	case operator_type::BLZ_IS_NULL:
	case operator_type::BLZ_IS_NOT_NULL:
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
		{"IS NOT TRUE", operator_type::BLZ_NOT},
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
		{"TRIM", operator_type::BLZ_STR_TRIM},
		{"LEFT", operator_type::BLZ_STR_LEFT},
		{"RIGHT", operator_type::BLZ_STR_RIGHT},
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

bool is_date(const std::string & token) {
	static const std::regex re{R"([0-9]{4}-[0-9]{2}-[0-9]{2})"};
	return std::regex_match(token, re);
}

bool is_hour(const std::string & token) {
	static const std::regex re{"([0-9]{2}):([0-9]{2}):([0-9]{2})"};
	return std::regex_match(token, re);
}

bool is_timestamp(const std::string & token) {
	static const std::regex re("([0-9]{4})-([0-9]{2})-([0-9]{2}) ([0-9]{2}):([0-9]{2}):([0-9]{2})");
	bool ret = std::regex_match(token, re);
	return ret;
}

bool is_bool(const std::string & token) { return (token == "true" || token == "false"); }

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
		   is_timestamp(token);
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

	// the default behavior when not bounds are passed is
	// RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW. 
	if (over_clause.find("BETWEEN") == std::string::npos) {
		preceding_value = -1;
		following_value = 0;
		return std::make_tuple(preceding_value, following_value);
	}

	// getting the first limit value
	std::string between_expr = "BETWEEN ";
	std::string preceding_expr = " PRECEDING";
	size_t start_pos = over_clause.find(between_expr) + between_expr.size();
	size_t end_pos = over_clause.find(preceding_expr);
	std::string first_limit = over_clause.substr(start_pos, end_pos - start_pos);
	preceding_value = std::stoi(first_limit);

	// getting the second limit value
	std::string and_expr = "AND ";
	std::string following_expr = " FOLLOWING";
	start_pos = over_clause.find(and_expr) + and_expr.size();
	end_pos = over_clause.find(following_expr);
	std::string second_limit = over_clause.substr(start_pos, end_pos - start_pos);
	following_value = std::stoi(second_limit);

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
// output: < [0, 4, 0], ["MIN", "MAX", "LEAD"], [3] >
std::tuple< std::vector<int>, std::vector<std::string>, std::vector<int> > 
get_cols_to_apply_window_and_cols_to_apply_agg(const std::string & logical_plan) {
	std::vector<int> column_index;
	std::vector<std::string> aggregations;
	std::vector<int> agg_param_values;

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
			} else {
				aggregations.push_back(split_parts[0]);
				std::string col_index = StringUtil::replace(split_parts[1], ")", "");
				column_index.push_back(std::stoi(col_index));
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
