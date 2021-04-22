#pragma once

#include <map>
#include <string>
#include <vector>
#include <cudf/types.hpp>
#include <cudf/aggregation.hpp>

enum class operator_type {
	BLZ_INVALID_OP,

	// Nullary operators
	BLZ_RAND,

	// Unary operators
	BLZ_NOT,
	BLZ_ABS,
	BLZ_FLOOR,
	BLZ_CEIL,
	BLZ_SIN,
	BLZ_COS,
	BLZ_ASIN,
	BLZ_ACOS,
	BLZ_TAN,
	BLZ_COTAN,
	BLZ_ATAN,
	BLZ_LN,
	BLZ_LOG,
	BLZ_YEAR,
	BLZ_MONTH,
	BLZ_DAY,
	BLZ_DAYOFWEEK,
	BLZ_HOUR,
	BLZ_MINUTE,
	BLZ_SECOND,
	BLZ_IS_NULL,
	BLZ_IS_NOT_NULL,
	BLZ_IS_TRUE,
	BLZ_IS_NOT_TRUE,  // matter when nulls
	BLZ_IS_NOT_FALSE, // matters when null
	BLZ_CAST_TINYINT,
	BLZ_CAST_SMALLINT,
	BLZ_CAST_INTEGER,
	BLZ_CAST_BIGINT,
	BLZ_CAST_FLOAT,
	BLZ_CAST_DOUBLE,
	BLZ_CAST_DATE,
	BLZ_CAST_TIMESTAMP,
	BLZ_CAST_VARCHAR,
	BLZ_CHAR_LENGTH,
	BLZ_STR_LOWER,
	BLZ_STR_UPPER,
	BLZ_STR_INITCAP,
	BLZ_STR_REVERSE,

	// Binary operators
	BLZ_ADD,            ///< operator +
	BLZ_SUB,            ///< operator -
	BLZ_MUL,            ///< operator *
	BLZ_DIV,            ///< operator / using common type of lhs and rhs
	BLZ_MOD,            ///< operator %
	BLZ_POW,            ///< lhs ^ rhs
	BLZ_ROUND,
	BLZ_EQUAL,          ///< operator ==
	BLZ_NOT_EQUAL,      ///< operator !=
	BLZ_LESS,           ///< operator <
	BLZ_GREATER,        ///< operator >
	BLZ_LESS_EQUAL,     ///< operator <=
	BLZ_GREATER_EQUAL,  ///< operator >=
	BLZ_BITWISE_AND,    ///< operator &
	BLZ_BITWISE_OR,     ///< operator |
	BLZ_BITWISE_XOR,    ///< operator ^
	BLZ_LOGICAL_AND,    ///< operator &&
	BLZ_LOGICAL_OR,     ///< operator ||
	BLZ_FIRST_NON_MAGIC,
	BLZ_MAGIC_IF_NOT,
	BLZ_STR_LIKE,
	BLZ_STR_SUBSTRING,
	BLZ_STR_REGEXP_REPLACE,
	BLZ_STR_CONCAT,
	BLZ_STR_REPLACE,
	BLZ_STR_LEFT,
	BLZ_STR_RIGHT,
	BLZ_TO_DATE,
	BLZ_TO_TIMESTAMP,
	BLZ_STR_TRIM,
	BLZ_IS_NOT_DISTINCT_FROM,
};


bool is_nullary_operator(operator_type op);
bool is_unary_operator(operator_type op);
bool is_binary_operator(operator_type op);

cudf::type_id get_output_type(operator_type op, cudf::type_id input_left_type);
cudf::type_id get_output_type(operator_type op, cudf::type_id input_left_type, cudf::type_id input_right_type);
cudf::type_id get_output_type(operator_type op);

operator_type map_to_operator_type(const std::string & operator_token);

bool is_null(const std::string & token);
bool is_number(const std::string & token);
bool is_hour(const std::string & token);
bool is_date_with_dash(const std::string & token);
bool is_date_with_bar(const std::string & token);
bool is_date(const std::string & token);
bool is_timestamp_with_dash(const std::string & token);
bool is_timestamp_with_bar(const std::string & token);
bool is_timestamp(const std::string & token);
bool is_timestamp_ms_with_dash(const std::string & token);
bool is_timestamp_ms_with_bar(const std::string & token);
bool is_timestamp_ms(const std::string & token);
bool is_timestamp_us_with_dash(const std::string & token);
bool is_timestamp_us_with_bar(const std::string & token);
bool is_timestamp_us(const std::string & token);
bool is_timestamp_ns_with_dash(const std::string & token);
bool is_timestamp_ns_with_bar(const std::string & token);
bool is_timestamp_ns(const std::string & token);
bool is_timestamp_with_decimals(const std::string & token);
bool is_timestamp_with_decimals_and_dash(const std::string & token);
bool is_timestamp_with_decimals_and_bar(const std::string & token);
bool is_string(const std::string & token);
bool is_bool(const std::string & token);
bool is_SQL_data_type(const std::string & token);
bool is_operator_token(const std::string & token);
bool is_literal(const std::string & token);
bool is_var_column(const std::string& token);
bool is_inequality(const std::string& token);

std::string get_named_expression(const std::string & query_part, const std::string & expression_name);

std::vector<int> get_projections(const std::string & query_part);

const std::string LOGICAL_JOIN_TEXT = "LogicalJoin";
const std::string LOGICAL_PARTWISE_JOIN_TEXT = "PartwiseJoin";
const std::string LOGICAL_JOIN_PARTITION_TEXT = "JoinPartition";
const std::string LOGICAL_UNION_TEXT = "LogicalUnion";
const std::string LOGICAL_SCAN_TEXT = "LogicalTableScan";
const std::string BINDABLE_SCAN_TEXT = "BindableTableScan";
const std::string LOGICAL_AGGREGATE_TEXT = "LogicalAggregate";  // this is the base Aggregate that gets replaced
const std::string LOGICAL_COMPUTE_AGGREGATE_TEXT = "ComputeAggregate";
const std::string LOGICAL_DISTRIBUTE_AGGREGATE_TEXT = "DistributeAggregate";
const std::string LOGICAL_MERGE_AGGREGATE_TEXT = "MergeAggregate";
const std::string LOGICAL_PROJECT_TEXT = "LogicalProject";
const std::string LOGICAL_LIMIT_TEXT = "LogicalLimit";
const std::string LOGICAL_SORT_TEXT = "LogicalSort";
const std::string LOGICAL_MERGE_TEXT = "LogicalMerge";
const std::string LOGICAL_PARTITION_TEXT = "LogicalPartition";
const std::string LOGICAL_SORT_AND_SAMPLE_TEXT = "Logical_SortAndSample";
const std::string LOGICAL_SINGLE_NODE_PARTITION_TEXT = "LogicalSingleNodePartition";
const std::string LOGICAL_FILTER_TEXT = "LogicalFilter";
const std::string LOGICAL_GENERATE_OVERLAPS_TEXT = "LogicalGenerateOverlaps";
const std::string LOGICAL_ACCUMULATE_OVERLAPS_TEXT = "LogicalAccumulateOverlaps";
const std::string LOGICAL_COMPUTE_WINDOW_TEXT = "LogicalComputeWindow";
const std::string ASCENDING_ORDER_SORT_TEXT = "ASC";
const std::string DESCENDING_ORDER_SORT_TEXT = "DESC";


bool is_union(std::string query_part);
bool is_project(std::string query_part);
bool is_logical_scan(std::string query_part);
bool is_bindable_scan(std::string query_part);
bool is_filtered_bindable_scan(std::string query_part);
bool is_scan(std::string query_part);
bool is_filter(std::string query_part);
bool is_limit(std::string query_part);
bool is_sort(std::string query_part);
bool is_merge(std::string query_part);
bool is_partition(std::string query_part);
bool is_sort_and_sample(std::string query_part);
bool is_single_node_partition(std::string query_part);
bool is_join(const std::string & query);
bool is_pairwise_join(const std::string & query);
bool is_join_partition(const std::string & query);
bool is_aggregate(std::string query_part); // this is the base Aggregate that gets replaced
bool is_compute_aggregate(std::string query_part);
bool is_distribute_aggregate(std::string query_part);
bool is_merge_aggregate(std::string query_part);
bool is_aggregate_merge(std::string query_part); // to be deprecated
bool is_aggregate_partition(std::string query_part); // to be deprecated
bool is_aggregate_and_sample(std::string query_part); // to be deprecated
bool is_window_function(std::string query_part);
bool is_generate_overlaps(std::string query_part);
bool is_accumulate_overlaps(std::string query_part);
bool is_window_compute(std::string query_part);

bool window_expression_contains_partition_by(std::string query_part);

bool window_expression_contains_order_by(std::string query_part);

bool window_expression_contains_bounds(std::string query_part);

bool window_expression_contains_bounds_by_range(std::string query_part);

bool is_lag_or_lead_aggregation(std::string expression);

bool is_first_value_window(std::string expression);

bool is_last_value_window(std::string expression);

bool window_expression_contains_multiple_diff_over_clauses(std::string query_part);

bool is_sum_window_function(std::string expression);

bool is_avg_window_function(std::string expression);

std::string remove_over_expr(std::string expression);

std::string replace_count_expr_with_right_index(std::string expression, size_t rigt_index);

std::string replace_sum0_expr_with_right_index(std::string expression, size_t rigt_index);

std::string get_query_part(std::string logical_plan);

std::tuple< int, int > get_bounds_from_window_expression(const std::string & logical_plan);

std::string get_frame_type_from_over_clause(const std::string & logical_plan);

std::string get_over_expression(std::string query_part);

std::string get_first_over_expression_from_logical_plan(const std::string & logical_plan, const std::string & expr);

std::tuple< std::vector<int>, std::vector<std::string>, std::vector<int> > 
get_cols_to_apply_window_and_cols_to_apply_agg(const std::string & query_part);

std::vector<std::string> clean_window_function_expressions(const std::vector<std::string> & expressions, size_t num_columns);

// Returns the index from table_scan if exists
size_t get_table_index(std::vector<std::string> table_scans, std::string table_scan);

// Input: [[hr, emps]] or [[emps]] Output: hr.emps or emps
std::string extract_table_name(std::string query_part);

// takes a comma delimited list of expressions and splits it into separate expressions
// if the flag trim is true, leading and trailing spaces are removed
std::vector<std::string> get_expressions_from_expression_list(std::string & combined_expressions, bool trim = true);

std::string replace_calcite_regex(const std::string & expression);

//Returns the column names according to the corresponding algebra expression
std::vector<std::string> fix_column_aliases(const std::vector<std::string> & column_names, std::string expression);

std::tuple< bool, bool, std::vector<std::string> > bypassingProject(std::string logical_plan, std::vector<std::string> names);

std::string fill_minus_op_with_zero(std::string expression);

std::string convert_concat_expression_into_multiple_binary_concat_ops(std::string expression);

const std::string remove_quotes_from_timestamp_literal(const std::string & scalar_string);
