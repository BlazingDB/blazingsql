#include "expression_utils.hpp"
#include "gdf_wrapper/gdf_types.cuh"
#include <map>
#include <regex>

bool is_binary_operator_token(const std::string & token) {
	return (gdf_binary_operator_map.find(token) != gdf_binary_operator_map.end());
}

bool is_unary_operator_token(const std::string & token) {
	return (gdf_unary_operator_map.find(token) != gdf_unary_operator_map.end());
}

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
	return std::find(std::begin(SQL_DATA_TYPES), std::end(SQL_DATA_TYPES), token) != std::end(SQL_DATA_TYPES);
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
