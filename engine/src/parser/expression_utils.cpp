#include <map>
#include <regex>

#include "expression_utils.hpp"
#include "Utils.cuh"

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
