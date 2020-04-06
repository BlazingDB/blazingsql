#include <map>
#include <regex>

#include "expression_utils.hpp"
#include "Utils.cuh"
#include <blazingdb/io/Util/StringUtil.h>

bool is_string(const std::string & token) { return token[0] == '\'' && token[token.size() - 1] == '\''; }

bool is_number(const std::string & token) {
	static const std::regex re{R""(^[-+]?[0-9]*\.?[0-9]+([eE][-+]?[0-9]+)?$)""};
	return std::regex_match(token, re);
}

bool is_null(const std::string & token) { return token == "null"; }

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

interops::operator_type map_to_operator_type(const std::string & operator_token) {
	// std::cout << "operator_token: " << operator_token << std::endl;
	RAL_EXPECTS(operator_map.find(operator_token) != operator_map.end(), "Unsupported operator: " + operator_token);

	return operator_map[operator_token];
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

bool is_sort(std::string query_part) { return (query_part.find(LOGICAL_SORT_TEXT) != std::string::npos); }

bool is_merge(std::string query_part) { return (query_part.find(LOGICAL_MERGE_TEXT) != std::string::npos); }

bool is_partition(std::string query_part) { return (query_part.find(LOGICAL_PARTITION_TEXT) != std::string::npos); }

bool is_sort_and_sample(std::string query_part) { return (query_part.find(LOGICAL_SORT_AND_SAMPLE_TEXT) != std::string::npos); }

bool is_join(const std::string & query) { return (query.find(LOGICAL_JOIN_TEXT) != std::string::npos); }

bool is_aggregate(std::string query_part) { return (query_part.find(LOGICAL_AGGREGATE_TEXT) != std::string::npos); }

bool is_aggregate_merge(std::string query_part) { return (query_part.find(LOGICAL_AGGREGATE_MERGE_TEXT) != std::string::npos); }

bool is_aggregate_partition(std::string query_part) { return (query_part.find(LOGICAL_AGGREGATE_PARTITION_TEXT) != std::string::npos); }

bool is_aggregate_and_sample(std::string query_part) { return (query_part.find(LOGICAL_AGGREGATE_AND_SAMPLE_TEXT) != std::string::npos); }

bool is_double_input(std::string query_part) {
	if(is_join(query_part)) {
		return true;
	} else if(is_union(query_part)) {
		return true;
	} else {
		return false;
	}
}



// Returns the index from table if exists
size_t get_table_index(std::vector<std::string> table_names, std::string table_name) {
	if(StringUtil::beginsWith(table_name, "main.")) {
		table_name = table_name.substr(5);
	}

	auto it = std::find(table_names.begin(), table_names.end(), table_name);
	if(it != table_names.end()) {
		return std::distance(table_names.begin(), it);
	} else {
		throw std::invalid_argument("table name does not exists ==>" + table_name);
	}
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

	static const std::regex number_implicit_cast_re{
		R""((\d):(?:DECIMAL\(\d+, \d+\)|INTEGER|BIGINT|FLOAT|DOUBLE))"", std::regex_constants::icase};
	ret = std::regex_replace(ret, number_implicit_cast_re, "$1");

	static const std::regex null_implicit_cast_re{
		R""(null:(?:DECIMAL\(\d+, \d+\)|INTEGER|BIGINT|FLOAT|DOUBLE))"", std::regex_constants::icase};
	ret = std::regex_replace(ret, null_implicit_cast_re, "null");

	static const std::regex varchar_implicit_cast_re{R""(':VARCHAR)"", std::regex_constants::icase};
	ret = std::regex_replace(ret, varchar_implicit_cast_re, "'");

	StringUtil::findAndReplaceAll(ret, "IS NOT NULL", "IS_NOT_NULL");
	StringUtil::findAndReplaceAll(ret, "IS NULL", "IS_NULL");
	StringUtil::findAndReplaceAll(ret, " NOT NULL", "");

	StringUtil::findAndReplaceAll(ret, "EXTRACT(FLAG(YEAR), ", "BL_YEAR(");
	StringUtil::findAndReplaceAll(ret, "EXTRACT(FLAG(MONTH), ", "BL_MONTH(");
	StringUtil::findAndReplaceAll(ret, "EXTRACT(FLAG(DAY), ", "BL_DAY(");
	StringUtil::findAndReplaceAll(ret, "EXTRACT(FLAG(HOUR), ", "BL_HOUR(");
	StringUtil::findAndReplaceAll(ret, "EXTRACT(FLAG(MINUTE), ", "BL_MINUTE(");
	StringUtil::findAndReplaceAll(ret, "EXTRACT(FLAG(SECOND), ", "BL_SECOND(");
	StringUtil::findAndReplaceAll(ret, ":DECIMAL(19, 0)", ":DOUBLE");


	StringUtil::findAndReplaceAll(ret, "/INT(", "/(");
	return ret;
}
