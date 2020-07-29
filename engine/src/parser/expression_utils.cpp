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
		return true;
	case operator_type::BLZ_STR_SUBSTRING:
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
	case operator_type::BLZ_CAST_VARCHAR:
		return cudf::type_id::STRING;
	case operator_type::BLZ_YEAR:
	case operator_type::BLZ_MONTH:
	case operator_type::BLZ_DAY:
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
	case operator_type::BLZ_STR_CONCAT:
		return cudf::type_id::STRING;
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
		{"CHAR_LENGTH", operator_type::BLZ_CHAR_LENGTH},

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
		{"||", operator_type::BLZ_STR_CONCAT}
	};

	RAL_EXPECTS(OPERATOR_MAP.find(operator_token) != OPERATOR_MAP.end(), "Unsupported operator");

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

std::vector<size_t> get_projections(const std::string & query_part) {
	std::string project_string = get_named_expression(query_part, "projects");
	std::vector<std::string> project_string_split =
		get_expressions_from_expression_list(project_string, true);

	std::vector<size_t> projections;
	for(int i = 0; i < project_string_split.size(); i++) {
		projections.push_back(std::stoull(project_string_split[i]));
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
	for(int i = 0; i < table_parts.size(); i++) {
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

	int curInd = 0;
	int curStart = 0;
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

	StringUtil::findAndReplaceAll(ret, "IS NOT NULL", "IS_NOT_NULL");
	StringUtil::findAndReplaceAll(ret, "IS NULL", "IS_NULL");
	StringUtil::findAndReplaceAll(ret, " NOT NULL", "");
	StringUtil::findAndReplaceAll(ret, "RAND()", "BLZ_RND()");
	StringUtil::findAndReplaceAll(ret, "EXTRACT(FLAG(YEAR), ", "BL_YEAR(");
	StringUtil::findAndReplaceAll(ret, "EXTRACT(FLAG(MONTH), ", "BL_MONTH(");
	StringUtil::findAndReplaceAll(ret, "EXTRACT(FLAG(DAY), ", "BL_DAY(");
	StringUtil::findAndReplaceAll(ret, "EXTRACT(FLAG(HOUR), ", "BL_HOUR(");
	StringUtil::findAndReplaceAll(ret, "EXTRACT(FLAG(MINUTE), ", "BL_MINUTE(");
	StringUtil::findAndReplaceAll(ret, "EXTRACT(FLAG(SECOND), ", "BL_SECOND(");

	StringUtil::findAndReplaceAll(ret, "/INT(", "/(");
	return ret;
}
