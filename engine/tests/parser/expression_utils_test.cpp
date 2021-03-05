#include "parser/expression_tree.hpp"
#include <gtest/gtest.h>
#include <iostream>

using namespace ral::parser;
struct ExpressionUtilsTest : public ::testing::Test {
	ExpressionUtilsTest() {}

	~ExpressionUtilsTest() {}
};

TEST_F(ExpressionUtilsTest, count_star_case) {

	// 'select count(*) from big_taxi' produces:
	std::string query_part = "BindableTableScan(table=[[main, big_taxi]], projects=[[]], aliases=[[$f0]])";

	std::vector<int> expected = {0}; // We want to load only one column, the first by default
	std::vector<int> result = get_projections(query_part);

	EXPECT_EQ(result, expected);
}

TEST_F(ExpressionUtilsTest, expression_contains_multiple_equal_over_clauses) {

	std::string query_part = "LogicalProject(max_prices=[MAX($0) OVER (PARTITION BY $2 ORDER BY $0, $1)], min_prices=[MIN($0) OVER (PARTITION BY $2 ORDER BY $0, $1)])";
	bool result = window_expression_contains_multiple_diff_over_clauses(query_part);

	EXPECT_EQ(result, false);
}

TEST_F(ExpressionUtilsTest, expression_contains_multiple_diff_over_clauses) {

	std::string query_part_1 = "LogicalProject(max_prices=[MAX($0) OVER (PARTITION BY $2 ORDER BY $0, $1)], min_prices=[MIN($0) OVER (PARTITION BY $2 ORDER BY $0)])";
	bool result_1 = window_expression_contains_multiple_diff_over_clauses(query_part_1);

	std::string query_part_2 = "LogicalProject(max_prices=[MAX($0) OVER (PARTITION BY $2)], min_prices=[MIN($0) OVER (PARTITION BY $3)])";
	bool result_2 = window_expression_contains_multiple_diff_over_clauses(query_part_2);

	EXPECT_EQ(result_1, true);
	EXPECT_EQ(result_2, true);
}

TEST_F(ExpressionUtilsTest, removing_over_expression) {

	std::string query_part_1 = "max_prices=[MAX($0) OVER (PARTITION BY $2, $4)]";
	std::string result_1 = remove_over_expr(query_part_1);
	std::string expected_1 = "max_prices=[MAX($0)]";

	std::string query_part_2 = "max_prices=[MAX($0) OVER (PARTITION BY $2 ORDER BY $0, $1)]";
	std::string result_2 = remove_over_expr(query_part_2);
	std::string expected_2 = "max_prices=[MAX($0)]";

	EXPECT_EQ(result_1, expected_1);
	EXPECT_EQ(result_2, expected_2);
}

TEST_F(ExpressionUtilsTest, getting_query_part) {

	std::string query_part_1 = "LogicalProject(sum_max_prices=[$0], o_orderkey=[$1], o_min_prices=[$2])";
	std::string result_1 = get_query_part(query_part_1);
	std::string expected_1 = "sum_max_prices=[$0], o_orderkey=[$1], o_min_prices=[$2]";

	std::string query_part_2 = "LogicalFilter(condition=[AND(<($0, 19750.0:DECIMAL(6, 1)), <>($6, 'Clerk#000000880'), OR(=($3, '2-HIGH'), =($3, '5-LOW')))])";
	std::string result_2 = get_query_part(query_part_2);
	std::string expected_2 = "condition=[AND(<($0, 19750.0:DECIMAL(6, 1)), <>($6, 'Clerk#000000880'), OR(=($3, '2-HIGH'), =($3, '5-LOW')))]";

	EXPECT_EQ(result_1, expected_1);
	EXPECT_EQ(result_2, expected_2);
}

TEST_F(ExpressionUtilsTest, getting_over_expression) {

	std::string query_part_1 = "min_keys=[MIN($0) OVER (PARTITION BY $1, $2 ORDER BY $0)]";
	std::string result_1 = get_over_expression(query_part_1);
	std::string expected_1 = "PARTITION BY $1, $2 ORDER BY $0";

	std::string query_part_2 = "sort0=[$1]";
	std::string result_2 = get_over_expression(query_part_2);
	std::string expected_2 = "";

	std::string query_part_3 = "sum_max_prices=[CASE(>(COUNT($0) OVER (PARTITION BY $3 ORDER BY $5, $4), 0), $SUM0($0) OVER (PARTITION BY $3 ORDER BY $5, $4), null:DOUBLE)]";
	std::string result_3 = get_over_expression(query_part_3);
	std::string expected_3 = "PARTITION BY $3 ORDER BY $5, $4";

	EXPECT_EQ(result_1, expected_1);
	EXPECT_EQ(result_2, expected_2);
	EXPECT_EQ(result_3, expected_3);
}

TEST_F(ExpressionUtilsTest, getting_frame_type_from_over_clause) {

	std::string query_part_1 = "min_keys=[MIN($0) OVER (PARTITION BY $1, $2 ORDER BY $0)]";
	std::string result_1 = get_frame_type_from_over_clause(query_part_1);
	std::string expected_1 = "RANGE";

	std::string query_part_2 = "max_keys=[MAX($0) OVER (PARTITION BY $1 ORDER BY $0 ROWS BETWEEN 1 PRECEDING AND 2 FOLLOWING)]";
	std::string result_2 = get_frame_type_from_over_clause(query_part_2);
	std::string expected_2 = "ROWS";

	EXPECT_EQ(result_1, expected_1);
	EXPECT_EQ(result_2, expected_2);
}

TEST_F(ExpressionUtilsTest, gettings_bounds_from_window_expression) {
	int preceding_value, following_value;
	int expected_presceding = -1;
	int expected_following = 0;
	std::string query_part_1 = "LogicalProject(min_keys=[MIN($0) OVER (PARTITION BY $1, $2 ORDER BY $0)])";
	std::tie(preceding_value, following_value) = get_bounds_from_window_expression(query_part_1);

	EXPECT_EQ(preceding_value, expected_presceding);
	EXPECT_EQ(following_value, expected_following);
	
	int expected_presceding2 = 1;
	int expected_following2 = 2;
	std::string query_part_2 = "max_keys=[MAX($0) OVER (PARTITION BY $1 ORDER BY $0 ROWS BETWEEN 1 PRECEDING AND 2 FOLLOWING)]";
	std::tie(preceding_value, following_value) = get_bounds_from_window_expression(query_part_2);

	EXPECT_EQ(preceding_value, expected_presceding2);
	EXPECT_EQ(following_value, expected_following2);	
}

TEST_F(ExpressionUtilsTest, getting_cols_to_apply_window_and_cols_to_apply_agg) {
	std::vector<int> column_indices_to_agg, agg_param_values;
	std::vector<std::string> type_aggs_as_str;

	std::string query_part = "LogicalComputeWindow(min_keys=[MIN($0) OVER (PARTITION BY $2 ORDER BY $1)], max_keys=[MAX($3) OVER (PARTITION BY $2 ORDER BY $1)])";
	std::tie(column_indices_to_agg, type_aggs_as_str, agg_param_values) = get_cols_to_apply_window_and_cols_to_apply_agg(query_part);

	std::vector<int> column_indices_expect = {0, 3}, agg_param_expect;
	std::vector<std::string> type_aggs_expect = {"MIN", "MAX"};

	EXPECT_EQ(column_indices_to_agg.size(), column_indices_expect.size());
	EXPECT_EQ(type_aggs_as_str.size(), type_aggs_expect.size());
	EXPECT_EQ(agg_param_values.size(), agg_param_expect.size());

	for (std::size_t i = 0; i < column_indices_to_agg.size(); ++i) {
		EXPECT_EQ(column_indices_to_agg[i], column_indices_expect[i]);
		EXPECT_EQ(type_aggs_as_str[i], type_aggs_expect[i]);
	}
}

TEST_F(ExpressionUtilsTest, getting_cols_to_apply_window_and_cols_to_apply_agg_empty) {
	std::vector<int> column_indices_to_agg, agg_param_values;
	std::vector<std::string> type_aggs_as_str;

	std::string query_part = "LogicalProject(o_min_prices=[$1], o_orderkey=[$2], o_custkey=[$4], o_clerk=[$6])";
	std::tie(column_indices_to_agg, type_aggs_as_str, agg_param_values) = get_cols_to_apply_window_and_cols_to_apply_agg(query_part);

	EXPECT_EQ(column_indices_to_agg.size(), 0);
	EXPECT_EQ(type_aggs_as_str.size(), 0);
	EXPECT_EQ(agg_param_values.size(), 0);
}
