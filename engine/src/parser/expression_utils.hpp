#pragma once

#include <map>
#include <string>
#include <vector>

#include "Interpreter/interpreter_cpp.h"

static std::map<std::string, interops::operator_type> operator_map = {
	// Unary operators
	{"NOT", interops::operator_type::BLZ_NOT},
	{"SIN", interops::operator_type::BLZ_SIN},
	{"ASIN", interops::operator_type::BLZ_ASIN},
	{"COS", interops::operator_type::BLZ_COS},
	{"ACOS", interops::operator_type::BLZ_ACOS},
	{"TAN", interops::operator_type::BLZ_TAN},
	{"ATAN", interops::operator_type::BLZ_ATAN},
	{"FLOOR", interops::operator_type::BLZ_FLOOR},
	{"CEIL", interops::operator_type::BLZ_CEIL},
	{"ABS", interops::operator_type::BLZ_ABS},
	{"LOG10", interops::operator_type::BLZ_LOG},
	{"LN", interops::operator_type::BLZ_LN},
	{"BL_YEAR", interops::operator_type::BLZ_YEAR},
	{"BL_MONTH", interops::operator_type::BLZ_MONTH},
	{"BL_DAY", interops::operator_type::BLZ_DAY},
	{"BL_HOUR", interops::operator_type::BLZ_HOUR},
	{"BL_MINUTE", interops::operator_type::BLZ_MINUTE},
	{"BL_SECOND", interops::operator_type::BLZ_SECOND},
	{"IS_NULL", interops::operator_type::BLZ_IS_NULL},
	{"IS_NOT_NULL", interops::operator_type::BLZ_IS_NOT_NULL},
	{"CAST_INTEGER", interops::operator_type::BLZ_CAST_INTEGER},
	{"CAST_BIGINT", interops::operator_type::BLZ_CAST_BIGINT},
	{"CAST_FLOAT", interops::operator_type::BLZ_CAST_FLOAT},
	{"CAST_DOUBLE", interops::operator_type::BLZ_CAST_DOUBLE},
	{"CAST_DATE", interops::operator_type::BLZ_CAST_DATE},
	{"CAST_TIMESTAMP", interops::operator_type::BLZ_CAST_TIMESTAMP},
	{"CAST_VARCHAR", interops::operator_type::BLZ_CAST_VARCHAR},
	
	// Binary operators
	{"=", interops::operator_type::BLZ_EQUAL},
	{"<>", interops::operator_type::BLZ_NOT_EQUAL},
	{">", interops::operator_type::BLZ_GREATER},
	{">=", interops::operator_type::BLZ_GREATER_EQUAL},
	{"<", interops::operator_type::BLZ_LESS},
	{"<=", interops::operator_type::BLZ_LESS_EQUAL},
	{"+", interops::operator_type::BLZ_ADD},
	{"-", interops::operator_type::BLZ_SUB},
	{"*", interops::operator_type::BLZ_MUL},
	{"/", interops::operator_type::BLZ_DIV},
	{"POWER", interops::operator_type::BLZ_POW},
	{"ROUND", interops::operator_type::BLZ_ROUND},
	{"MOD", interops::operator_type::BLZ_MOD},
	{"AND", interops::operator_type::BLZ_LOGICAL_AND},
	{"OR", interops::operator_type::BLZ_LOGICAL_OR},
	{"FIRST_NON_MAGIC", interops::operator_type::BLZ_FIRST_NON_MAGIC},
	{"MAGIC_IF_NOT", interops::operator_type::BLZ_MAGIC_IF_NOT},
	{"LIKE", interops::operator_type::BLZ_STR_LIKE},
	{"SUBSTRING", interops::operator_type::BLZ_STR_SUBSTRING},
	{"||", interops::operator_type::BLZ_STR_CONCAT}
};

static std::vector<std::string> CALCITE_DATA_TYPES = {
	"INTEGER", "BIGINT", "FLOAT", "DOUBLE", "DATE", "TIMESTAMP", "VARCHAR"};

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

bool is_var_column(const std::string& token);

bool is_inequality(const std::string& token);

std::string get_named_expression(const std::string & query_part, const std::string & expression_name);

interops::operator_type map_to_operator_type(const std::string & operator_token);


const std::string LOGICAL_JOIN_TEXT = "LogicalJoin";
const std::string LOGICAL_UNION_TEXT = "LogicalUnion";
const std::string LOGICAL_SCAN_TEXT = "LogicalTableScan";
const std::string BINDABLE_SCAN_TEXT = "BindableTableScan";
const std::string LOGICAL_AGGREGATE_TEXT = "LogicalAggregate";
const std::string LOGICAL_AGGREGATE_MERGE_TEXT = "Logical_AggregateMerge";
const std::string LOGICAL_AGGREGATE_PARTITION_TEXT = "Logical_AggregatePartition";
const std::string LOGICAL_AGGREGATE_AND_SAMPLE_TEXT = "Logical_AggregateAndSample";
const std::string LOGICAL_PROJECT_TEXT = "LogicalProject";
const std::string LOGICAL_SORT_TEXT = "LogicalSort";
const std::string LOGICAL_MERGE_TEXT = "LogicalMerge";
const std::string LOGICAL_PARTITION_TEXT = "LogicalPartition";
const std::string LOGICAL_SORT_AND_SAMPLE_TEXT = "Logical_SortAndSample";
const std::string LOGICAL_FILTER_TEXT = "LogicalFilter";
const std::string ASCENDING_ORDER_SORT_TEXT = "ASC";
const std::string DESCENDING_ORDER_SORT_TEXT = "DESC";


bool is_union(std::string query_part); 
bool is_project(std::string query_part); 
bool is_logical_scan(std::string query_part); 
bool is_bindable_scan(std::string query_part); 
bool is_filtered_bindable_scan(std::string query_part); 
bool is_scan(std::string query_part); 
bool is_filter(std::string query_part);
bool is_sort(std::string query_part);
bool is_merge(std::string query_part);
bool is_partition(std::string query_part);
bool is_sort_and_sample(std::string query_part);
bool is_join(const std::string & query);
bool is_aggregate(std::string query_part);
bool is_aggregate_merge(std::string query_part);
bool is_aggregate_partition(std::string query_part);
bool is_aggregate_and_sample(std::string query_part);

bool is_double_input(std::string query_part);


// Returns the index from table if exists
size_t get_table_index(std::vector<std::string> table_names, std::string table_name);

// Input: [[hr, emps]] or [[emps]] Output: hr.emps or emps
std::string extract_table_name(std::string query_part);

std::vector<std::string> get_expressions_from_expression_list(std::string & combined_expression, bool trim);

std::string replace_calcite_regex(const std::string & expression);
