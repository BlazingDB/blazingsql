#pragma once

#include "gdf_wrapper/gdf_types.cuh"
#include <map>
#include <string>
#include <vector>


static std::map<std::string, gdf_unary_operator> gdf_unary_operator_map = {{"NOT", BLZ_NOT},
	{"SIN", BLZ_SIN},
	{"ASIN", BLZ_ASIN},
	{"COS", BLZ_COS},
	{"ACOS", BLZ_ACOS},
	{"TAN", BLZ_TAN},
	{"ATAN", BLZ_ATAN},
	{"BL_FLOUR", BLZ_FLOOR},
	{"CEIL", BLZ_CEIL},
	{"ABS", BLZ_ABS},
	{"LOG10", BLZ_LOG},
	{"LN", BLZ_LN},
	{"BL_YEAR", BLZ_YEAR},
	{"BL_MONTH", BLZ_MONTH},
	{"BL_DAY", BLZ_DAY},
	{"BL_HOUR", BLZ_HOUR},
	{"BL_MINUTE", BLZ_MINUTE},
	{"BL_SECOND", BLZ_SECOND},
	{"IS_NULL", BLZ_IS_NULL},
	{"IS_NOT_NULL", BLZ_IS_NOT_NULL},
	{"CAST_INTEGER", BLZ_CAST_INTEGER},
	{"CAST_BIGINT", BLZ_CAST_BIGINT},
	{"CAST_FLOAT", BLZ_CAST_FLOAT},
	{"CAST_DOUBLE", BLZ_CAST_DOUBLE},
	{"CAST_DATE", BLZ_CAST_DATE},
	{"CAST_TIMESTAMP", BLZ_CAST_TIMESTAMP},
	{"CAST_VARCHAR", BLZ_CAST_VARCHAR}};


static std::map<std::string, gdf_binary_operator_exp> gdf_binary_operator_map = {{"=", BLZ_EQUAL},
	{"<>", BLZ_NOT_EQUAL},
	{">", BLZ_GREATER},
	{">=", BLZ_GREATER_EQUAL},
	{"<", BLZ_LESS},
	{"<=", BLZ_LESS_EQUAL},
	{"+", BLZ_ADD},
	{"-", BLZ_SUB},
	{"*", BLZ_MUL},
	{"/", BLZ_DIV},
	{"POWER", BLZ_POW},
	{"MOD", BLZ_MOD},
	{"AND", BLZ_MUL},
	{"OR", BLZ_LOGICAL_OR},
	{"COALESCE", BLZ_COALESCE},
	{"FIRST_NON_MAGIC", BLZ_FIRST_NON_MAGIC},
	{"MAGIC_IF_NOT", BLZ_MAGIC_IF_NOT},
	{"LIKE", BLZ_STR_LIKE},
	{"SUBSTRING", BLZ_STR_SUBSTRING},
	{"||", BLZ_STR_CONCAT}};

static std::vector<std::string> SQL_DATA_TYPES = {
	"INTEGER", "BIGINT", "FLOAT", "DOUBLE", "DATE", "TIMESTAMP", "VARCHAR"};

bool is_binary_operator_token(const std::string & token);

bool is_unary_operator_token(const std::string & token);

bool is_number(const std::string & token);

bool is_null(const std::string & token);

bool is_date(const std::string & token);

bool is_hour(const std::string & token);

bool is_timestamp(const std::string & token);

bool is_string(const std::string & token);

bool is_bool(const std::string & token);

bool is_SQL_data_type(const std::string & token);

bool is_operator_token(const std::string & token);

bool is_literal(const std::string & token);

bool is_var_column(const std::string & token);
