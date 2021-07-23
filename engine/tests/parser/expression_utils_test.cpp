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
	int expected_preceding = -1;
	int expected_following = 0;
	std::string query_part_1 = "LogicalProject(min_keys=[MIN($0) OVER (PARTITION BY $1, $2 ORDER BY $0)])";
	std::tie(preceding_value, following_value) = get_bounds_from_window_expression(query_part_1);

	EXPECT_EQ(preceding_value, expected_preceding);
	EXPECT_EQ(following_value, expected_following);
	
	std::vector<std::string> preceding_options ={"UNBOUNDED PRECEDING", "4 PRECEDING", "CURRENT ROW"};
	std::vector<std::string> following_options ={"UNBOUNDED FOLLOWING", "4 FOLLOWING", "CURRENT ROW"};
	std::vector<int> expected = {-1, 4, 0};
	for (int i = 0; i < preceding_options.size(); ++i) {
		for (int j = 0; j < following_options.size(); ++j) {
			if (!(preceding_options[i] == "CURRENT ROW" && following_options[j] == "CURRENT ROW")){
				int expected_preceding2 = expected[i];
				int expected_following2 = expected[j];
				std::string query_part_2 = "max_keys=[MAX($0) OVER (PARTITION BY $1 ORDER BY $0 ROWS BETWEEN " +  preceding_options[i] + " AND " + following_options[j] + ")]";
				std::tie(preceding_value, following_value) = get_bounds_from_window_expression(query_part_2);
				EXPECT_EQ(preceding_value, expected_preceding2);
				EXPECT_EQ(following_value, expected_following2);
			}
		}
	}		
}

TEST_F(ExpressionUtilsTest, getting_cols_to_apply_window_and_cols_to_apply_agg) {
	std::vector<int> column_indices_to_agg, agg_param_values;
	std::vector<std::string> type_aggs_as_str;

	std::string query_part = "LogicalComputeWindow(min_keys=[MIN($0) OVER (PARTITION BY $2 ORDER BY $1)], max_keys=[MAX($3) OVER (PARTITION BY $2 ORDER BY $1)])";
	std::tie(column_indices_to_agg, type_aggs_as_str, agg_param_values) = get_cols_to_apply_window_and_cols_to_apply_agg(query_part);

	std::vector<int> column_indices_expect = {0, 3};
	std::vector<int> agg_param_expect = {0, 0};
	std::vector<std::string> type_aggs_expect = {"MIN", "MAX"};

	EXPECT_EQ(column_indices_to_agg.size(), column_indices_expect.size());
	EXPECT_EQ(type_aggs_as_str.size(), type_aggs_expect.size());
	EXPECT_EQ(agg_param_values.size(), agg_param_expect.size());

	for (int i = 0; i < column_indices_to_agg.size(); ++i) {
		EXPECT_EQ(column_indices_to_agg[i], column_indices_expect[i]);
		EXPECT_EQ(type_aggs_as_str[i], type_aggs_expect[i]);
		EXPECT_EQ(agg_param_values[i], agg_param_expect[i]);
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

TEST_F(ExpressionUtilsTest, by_passing_project) {
	bool by_passing_project, by_passing_project_with_aliases;
	std::vector<std::string> aliases;
	
	std::string logical_plan = "LogicalProject(o_orderkey=[$0], o_custkey=[$1], o_orderstatus=[$2], o_totalprice=[$3])";
	std::vector<std::string> col_names = {"o_orderkey", "o_custkey", "o_orderstatus", "o_totalprice"};
	std::tie(by_passing_project, by_passing_project_with_aliases, aliases) = bypassingProject(logical_plan, col_names);

	EXPECT_EQ(by_passing_project, true);
	EXPECT_EQ(by_passing_project_with_aliases, false);
}

TEST_F(ExpressionUtilsTest, not_passing_project) {
	bool by_passing_project, by_passing_project_with_aliases;
	std::vector<std::string> aliases;
	
	std::string logical_plan = "LogicalProject(o_orderpriority=[$5], o_custkey=[$1], o_orderstatus=[$2])";
	std::vector<std::string> col_names = {"o_orderkey", "o_custkey", "o_orderstatus", "o_totalprice", "o_orderdate", "o_orderpriority"};
	std::tie(by_passing_project, by_passing_project_with_aliases, aliases) = bypassingProject(logical_plan, col_names);

	EXPECT_EQ(by_passing_project, false);
	EXPECT_EQ(by_passing_project_with_aliases, false);
}

TEST_F(ExpressionUtilsTest, by_passing_project_with_aliases) {
	bool by_passing_project, by_passing_project_with_aliases;
	std::vector<std::string> aliases;
	
	std::string logical_plan = "LogicalProject(alias_0=[$0], alias_1=[$1], alias_2=[$2], alias_3=[$3])";
	std::vector<std::string> col_names = {"o_orderkey", "o_custkey", "o_orderstatus", "o_totalprice"};
	std::tie(by_passing_project, by_passing_project_with_aliases, aliases) = bypassingProject(logical_plan, col_names);
	std::vector<std::string> expected_aliases = {"alias_0", "alias_1", "alias_2", "alias_3"};

	EXPECT_EQ(by_passing_project, true);
	EXPECT_EQ(by_passing_project_with_aliases, true);
	EXPECT_EQ(aliases.size(), expected_aliases.size());

	for (int i = 0; i < aliases.size(); ++i) {
		EXPECT_EQ(aliases[i], expected_aliases[i]);
	}
}

TEST_F(ExpressionUtilsTest, not_passing_project_due_to_sum_operation) {
	bool by_passing_project, by_passing_project_with_aliases;
	std::vector<std::string> aliases;
	
	std::string logical_plan = "LogicalProject(alias_0=[+($0, 1)], alias_1=[$1], alias_2=[$2], alias_3=[$3])";
	std::vector<std::string> col_names = {"o_orderkey", "o_custkey", "o_orderstatus", "o_totalprice"};
	std::tie(by_passing_project, by_passing_project_with_aliases, aliases) = bypassingProject(logical_plan, col_names);

	EXPECT_EQ(by_passing_project, false);
	EXPECT_EQ(by_passing_project_with_aliases, false);
}

TEST_F(ExpressionUtilsTest, not_passing_project_empty_plan) {
	bool by_passing_project, by_passing_project_with_aliases;
	std::vector<std::string> aliases;
	
	std::string logical_plan = "";
	std::vector<std::string> col_names = {"o_orderkey", "o_custkey", "o_orderstatus"};
	std::tie(by_passing_project, by_passing_project_with_aliases, aliases) = bypassingProject(logical_plan, col_names);

	EXPECT_EQ(by_passing_project, false);
	EXPECT_EQ(by_passing_project_with_aliases, false);
}

TEST_F(ExpressionUtilsTest, not_passing_project_empty_col_names) {
	bool by_passing_project, by_passing_project_with_aliases;
	std::vector<std::string> aliases;
	
	std::string logical_plan = "LogicalProject(o_orderkey=[$0], o_custkey=[$1], o_orderstatus=[$2])";
	std::vector<std::string> col_names;
	std::tie(by_passing_project, by_passing_project_with_aliases, aliases) = bypassingProject(logical_plan, col_names);

	EXPECT_EQ(by_passing_project, false);
	EXPECT_EQ(by_passing_project_with_aliases, false);
}

TEST_F(ExpressionUtilsTest, filling_minus_op_with_zero_not_apply_case1) {
	std::string expression = "-(4, $3)";
	std::string expression_result = fill_minus_op_with_zero(expression);

	EXPECT_EQ(expression_result, expression);
}

TEST_F(ExpressionUtilsTest, filling_minus_op_with_zero_not_apply_case2) {
	std::string expression = "-($0, $3)";
	std::string expression_result = fill_minus_op_with_zero(expression);

	EXPECT_EQ(expression_result, expression);
}

TEST_F(ExpressionUtilsTest, filling_minus_op_with_zero_not_apply_case3) {
	std::string expression = "-(-($0, $1), $0)";
	std::string expression_result = fill_minus_op_with_zero(expression);

	EXPECT_EQ(expression_result, expression);
}

TEST_F(ExpressionUtilsTest, filling_minus_op_with_zero_success) {
	std::string expression = "-($3)";
	std::string expression_result = fill_minus_op_with_zero(expression);
	std::string expected_expression = "-(0, $3)";

	EXPECT_EQ(expression_result, expected_expression);
}

TEST_F(ExpressionUtilsTest, filling_minus_op_with_zero_success_with_cast) {
	std::string expression = "-(CAST($0):DOUBLE)";
	std::string expression_result = fill_minus_op_with_zero(expression);
	std::string expected_expression = "-(0, CAST($0):DOUBLE)";

	EXPECT_EQ(expression_result, expected_expression);
}

TEST_F(ExpressionUtilsTest, concat_operator_with_empty_expressions)
{
	try {
		std::string expression = "CONCAT()";
		std::string out_expression = convert_nary_to_binary_concat(expression);

		FAIL();
	} catch(const std::exception& e) {
		SUCCEED();
	}
}

TEST_F(ExpressionUtilsTest, concat_operator_wo_literals_expressions)
{
	std::string expression = "CONCAT($0, $1)";
	std::string out_expression = convert_nary_to_binary_concat(expression);

	EXPECT_EQ(out_expression, expression);
}

TEST_F(ExpressionUtilsTest, concat_operator_with_one_literal_expressions)
{
	std::string expression = "CONCAT($0, '-ab25')";
	std::string out_expression = convert_nary_to_binary_concat(expression);
	EXPECT_EQ(out_expression, expression);
}

TEST_F(ExpressionUtilsTest, concat_operator_with_multiple_literal_expressions)
{
	std::string expression = "CONCAT(' - ', $1, ' : ')";
	std::string out_expression = convert_nary_to_binary_concat(expression);
	std::string expected_str = "CONCAT(CONCAT(' - ', $1), ' : ')";

	EXPECT_EQ(out_expression, expected_str);
}

TEST_F(ExpressionUtilsTest, concat_operator_using_cast_op)
{
	std::string expression = "CONCAT($0, ': ', CAST($1):VARCHAR, ' - ', $2)";
	std::string out_expression = convert_nary_to_binary_concat(expression);
	std::string expected_str = "CONCAT(CONCAT(CONCAT(CONCAT($0, ': '), CAST($1):VARCHAR), ' - '), $2)";

	EXPECT_EQ(out_expression, expected_str);
}

TEST_F(ExpressionUtilsTest, concat_operator_using_comma_as_literal)
{
	std::string expression = "CONCAT($0, ' , ', $2)";
	std::string out_expression = convert_nary_to_binary_concat(expression);
	std::string expected_str = "CONCAT(CONCAT($0, ' , '), $2)";

	EXPECT_EQ(out_expression, expected_str);
}

TEST_F(ExpressionUtilsTest, concat_operator_using_binary_cast_op)
{
	std::string expression = "CONCAT($1, '-', CAST(+($0, 1)):VARCHAR)";
	std::string out_expression = convert_nary_to_binary_concat(expression);
	std::string expected_str = "CONCAT(CONCAT($1, '-'), CAST(+($0, 1)):VARCHAR)";

	EXPECT_EQ(out_expression, expected_str);
}

TEST_F(ExpressionUtilsTest, concat_operator_into_another_operator) {
	std::string expression =
		"OPERATOR(CONCAT('Customer#000000', CAST($0):VARCHAR), 'Customer#0000001', "
		"CONCAT('=order', CAST($901):STRING, 'another+op'))";
	std::string out_expr = convert_nary_to_binary_concat(expression);
	EXPECT_EQ(out_expr,
		"OPERATOR(CONCAT('Customer#000000', CAST($0):VARCHAR), 'Customer#0000001', CONCAT(CONCAT('=order', "
		"CAST($901):STRING), 'another+op'))");
}

TEST_F(ExpressionUtilsTest, concat_operator_inside_a_like_operator)
{
	std::string expression = "LIKE(CONCAT('Customer#000000', $0), 'Customer#0000001')";
	std::string out_expression = convert_nary_to_binary_concat(expression);

	EXPECT_EQ(out_expression, expression);
}

TEST_F(ExpressionUtilsTest, multiple_concat_operator_inside_a_like_operator)
{
	std::string expression = "LIKE(CONCAT('abcd', '0000', $0), '001')";
	std::string out_expression = convert_nary_to_binary_concat(expression);
	std::string expected_str = "LIKE(CONCAT(CONCAT('abcd', '0000'), $0), '001')";

	EXPECT_EQ(out_expression, expected_str);
}

TEST_F(ExpressionUtilsTest, replace_is_not_distinct_as_calcite__empty)
{
	std::string expression = "";
	std::string out_expression = replace_is_not_distinct_as_calcite(expression);

	EXPECT_EQ(out_expression, expression);
}

TEST_F(ExpressionUtilsTest, replace_is_not_distinct_as_calcite__not_contains)
{
	std::string expression = "CONCAT($0, ' , ', $2)";
	std::string out_expression = replace_is_not_distinct_as_calcite(expression);

	EXPECT_EQ(out_expression, expression);
}

TEST_F(ExpressionUtilsTest, replace_is_not_distinct_as_calcite__bad_expression)
{
	std::string expression = "IS NOT DISTINCT FROM($1, $3)";
	std::string out_expression = replace_is_not_distinct_as_calcite(expression);

	EXPECT_EQ(out_expression, expression);
}

TEST_F(ExpressionUtilsTest, replace_is_not_distinct_as_calcite__contains)
{
	std::string expression = "IS_NOT_DISTINCT_FROM($1, $3)";
	std::string out_expression = replace_is_not_distinct_as_calcite(expression);
	std::string expected_str = "OR(AND(IS NULL($1), IS NULL($3)), IS TRUE(=($1 , $3)))";

	EXPECT_EQ(out_expression, expected_str);
}

TEST_F(ExpressionUtilsTest, reinterpreting_timestamp_ms_complex)
{
	std::string expression = "CAST(/INT(Reinterpret(-(1996-12-01 12:00:01, $2)), 86400000)):INTEGER";
	std::vector<cudf::data_type> schema {cudf::data_type{cudf::type_id::INT32},
										 cudf::data_type{cudf::type_id::STRING},
										 cudf::data_type{cudf::type_id::TIMESTAMP_MILLISECONDS}};
	std::string out_expression = reinterpret_timestamp(expression, schema);
	std::string expected_str = "CAST(/INT(Reinterpret(-(CAST(1996-12-01 12:00:01):TIMESTAMP_MILLISECONDS, $2)), 86400000)):INTEGER";

	EXPECT_EQ(out_expression, expected_str);
}

TEST_F(ExpressionUtilsTest, replace_is_not_distinct_as_calcite__wrong_expression)
{
	try {
		std::string expression = "IS_NOT_DISTINCT_FROM($1, $3, $5)";
		std::string out_expression = replace_is_not_distinct_as_calcite(expression);
		FAIL();
	} catch(const std::exception& e) {
		SUCCEED();
	}
}

// update_join_filter: update_join_and_filter_expressions_from_is_not_distinct_expr
TEST_F(ExpressionUtilsTest, update_join_filter__empty)
{
	std::string condition = "";
	std::string join_expres, filter_expres;
	std::tie(join_expres, filter_expres) = update_join_and_filter_expressions_from_is_not_distinct_expr(condition); 

	EXPECT_EQ(join_expres, "");
	EXPECT_EQ(filter_expres, "");
}

TEST_F(ExpressionUtilsTest, update_join_filter__not_contains)
{
	std::string condition = "=($0, $3)";
	std::string join_expres, filter_expres;
	std::tie(join_expres, filter_expres) = update_join_and_filter_expressions_from_is_not_distinct_expr(condition);

	EXPECT_EQ(join_expres, condition);
	EXPECT_EQ(filter_expres, "");
}

TEST_F(ExpressionUtilsTest, update_join_filter__not_contains_and)
{
	std::string condition = "=($0, $3), IS_NOT_DISTINCT_FROM($1, $3)";
	std::string join_expres, filter_expres;
	std::tie(join_expres, filter_expres) = update_join_and_filter_expressions_from_is_not_distinct_expr(condition);

	EXPECT_EQ(join_expres, condition);
	EXPECT_EQ(filter_expres, "");
}

TEST_F(ExpressionUtilsTest, update_join_filter__not_contains_is_not_distinct)
{
	std::string condition = " AND(=($0, $3), $3 < 1254)";
	std::string join_expres, filter_expres;
	std::tie(join_expres, filter_expres) = update_join_and_filter_expressions_from_is_not_distinct_expr(condition);

	EXPECT_EQ(join_expres, condition);
	EXPECT_EQ(filter_expres, "");
}

TEST_F(ExpressionUtilsTest, update_join_filter__right_express)
{
	std::string condition = "AND(=($0, $3), IS_NOT_DISTINCT_FROM($1, $3))";
	std::string join_expres, filter_expres;
	std::tie(join_expres, filter_expres) = update_join_and_filter_expressions_from_is_not_distinct_expr(condition); 

	EXPECT_EQ(join_expres, "=($0, $3)");
	EXPECT_EQ(filter_expres, "OR(AND(IS NULL($1), IS NULL($3)), IS TRUE(=($1 , $3)))");
}

TEST_F(ExpressionUtilsTest, update_join_filter__multiple_is_not_distinct_from_express)
{
	std::string condition = "AND(=($0, $3), IS_NOT_DISTINCT_FROM($1, $3), IS_NOT_DISTINCT_FROM($2, $5))";
	std::string join_expres, filter_expres;
	std::tie(join_expres, filter_expres) = update_join_and_filter_expressions_from_is_not_distinct_expr(condition); 

	EXPECT_EQ(join_expres, "=($0, $3)");
	EXPECT_EQ(filter_expres, "AND(OR(AND(IS NULL($1), IS NULL($3)), IS TRUE(=($1 , $3))), OR(AND(IS NULL($2), IS NULL($5)), IS TRUE(=($2 , $5))))");
}

TEST_F(ExpressionUtilsTest, update_join_filter__multiple_conditions)
{
	std::string condition = "AND(=($0, $4), >($3, $6), IS NOT IS_NOT_DISTINCT_FROM FROM($2, $5))";
	std::string join_expres, filter_expres;
	std::tie(join_expres, filter_expres) = update_join_and_filter_expressions_from_is_not_distinct_expr(condition); 

	EXPECT_EQ(join_expres, "=($0, $4)");
	EXPECT_EQ(filter_expres, "AND(>($3, $6), OR(AND(IS NULL($2), IS NULL($5)), IS TRUE(=($2 , $5))))");
}

TEST_F(ExpressionUtilsTest, update_join_filter__multiple_conditions_unordered)
{
	std::string condition = "AND(>($3, $6), =($0, $4), IS_NOT_DISTINCT_FROM($2, $5))";
	std::string join_expres, filter_expres;
	std::tie(join_expres, filter_expres) = update_join_and_filter_expressions_from_is_not_distinct_expr(condition); 

	EXPECT_EQ(join_expres, "=($0, $4)");
	EXPECT_EQ(filter_expres, "AND(>($3, $6), OR(AND(IS NULL($2), IS NULL($5)), IS TRUE(=($2 , $5))))");
}

TEST_F(ExpressionUtilsTest, convert_ms_to_ns_units_empty_expr)
{
	std::string expression = "";
	std::string out_expression = convert_ms_to_ns_units(expression);
}

TEST_F(ExpressionUtilsTest, convert_ms_to_ns_units_add_day)
{
	std::string expression = "+(CAST($0):TIMESTAMP, 1468800000:INTERVAL DAY)";
	std::string out_expression = convert_ms_to_ns_units(expression);
	std::string expected_str = "+(CAST($0):TIMESTAMP, 1468800000000000:INTERVAL DAY)";

	EXPECT_EQ(out_expression, expected_str);
}

TEST_F(ExpressionUtilsTest, convert_ms_to_ns_units_add_hour)
{
	std::string expression = "+(CAST($0):TIMESTAMP, 172800000:INTERVAL HOUR)";
	std::string out_expression = convert_ms_to_ns_units(expression);
	std::string expected_str = "+(CAST($0):TIMESTAMP, 172800000000000:INTERVAL HOUR)";

	EXPECT_EQ(out_expression, expected_str);
}

TEST_F(ExpressionUtilsTest, convert_ms_to_ns_units_add_minute)
{
	std::string expression = "+(CAST($0):TIMESTAMP(0), 4500000:INTERVAL MINUTE)";
	std::string out_expression = convert_ms_to_ns_units(expression);
	std::string expected_str = "+(CAST($0):TIMESTAMP(0), 4500000000000:INTERVAL MINUTE)";

	EXPECT_EQ(out_expression, expected_str);
}

TEST_F(ExpressionUtilsTest, convert_ms_to_ns_units_add_second)
{
	std::string expression = "+(CAST($0):TIMESTAMP, 150000:INTERVAL SECOND)";
	std::string out_expression = convert_ms_to_ns_units(expression);
	std::string expected_str = "+(CAST($0):TIMESTAMP, 150000000000:INTERVAL SECOND)";

	EXPECT_EQ(out_expression, expected_str);
}

TEST_F(ExpressionUtilsTest, convert_ms_to_ns_units_diff_day)
{
	std::string expression = "CAST(/INT(Reinterpret(-(2020-10-15 10:58:02, CAST($0):TIMESTAMP(0))), 86400000)):INTEGER";
	std::string out_expression = convert_ms_to_ns_units(expression);
	std::string expected_str = "CAST(/INT(Reinterpret(-(2020-10-15 10:58:02, CAST($0):TIMESTAMP(0))), 86400000000000)):INTEGER";

	EXPECT_EQ(out_expression, expected_str);
}

TEST_F(ExpressionUtilsTest, convert_ms_to_ns_units_diff_hour)
{
	std::string expression = "CAST(/INT(Reinterpret(-(1996-12-01 12:00:01, CAST($0):TIMESTAMP(0))), 3600000)):INTEGER";
	std::string out_expression = convert_ms_to_ns_units(expression);
	std::string expected_str = "CAST(/INT(Reinterpret(-(1996-12-01 12:00:01, CAST($0):TIMESTAMP(0))), 3600000000000)):INTEGER";

	EXPECT_EQ(out_expression, expected_str);
}

TEST_F(ExpressionUtilsTest, convert_ms_to_ns_units_diff_minute)
{
	std::string expression = "CAST(/INT(Reinterpret(-(1996-12-01 12:00:01, CAST($0):TIMESTAMP(0))), 60000)):INTEGER";
	std::string out_expression = convert_ms_to_ns_units(expression);
	std::string expected_str = "CAST(/INT(Reinterpret(-(1996-12-01 12:00:01, CAST($0):TIMESTAMP(0))), 60000000000)):INTEGER";

	EXPECT_EQ(out_expression, expected_str);
}

TEST_F(ExpressionUtilsTest, convert_ms_to_ns_units_diff_second)
{
	std::string expression = "CAST(/INT(Reinterpret(-(1996-12-01 12:00:01, CAST($0):TIMESTAMP(0))), 1000)):INTEGER";
	std::string out_expression = convert_ms_to_ns_units(expression);
	std::string expected_str = "CAST(/INT(Reinterpret(-(1996-12-01 12:00:01, CAST($0):TIMESTAMP(0))), 1000000000)):INTEGER";

	EXPECT_EQ(out_expression, expected_str);
}

TEST_F(ExpressionUtilsTest, reinterpreting_timestamp_empty_expression)
{
	std::string expression = "";
	std::vector<cudf::data_type> schema {cudf::data_type{cudf::type_id::TIMESTAMP_SECONDS}};
	std::string out_expression = reinterpret_timestamp(expression, schema);

	EXPECT_EQ(out_expression, expression);
}

TEST_F(ExpressionUtilsTest, reinterpreting_timestamp_empty_schema)
{
	std::string expression = "+(CAST($0):TIMESTAMP(0), 8000:INTERVAL SECOND)";
	std::vector<cudf::data_type> schema;
	std::string out_expression = reinterpret_timestamp(expression, schema);

	EXPECT_EQ(out_expression, expression);
}

TEST_F(ExpressionUtilsTest, reinterpreting_timestamp_s)
{
	std::string expression = "Reinterpret(-(1996-12-01 12:00:01, $0))";
	std::vector<cudf::data_type> schema {cudf::data_type{cudf::type_id::TIMESTAMP_SECONDS}};
	std::string out_expression = reinterpret_timestamp(expression, schema);
	std::string expected_str = "Reinterpret(-(CAST(1996-12-01 12:00:01):TIMESTAMP_SECONDS, $0))";

	EXPECT_EQ(out_expression, expected_str);
}

TEST_F(ExpressionUtilsTest, reinterpreting_timestamp_ms)
{
	std::string expression = "Reinterpret(-(1996-12-01 12:00:01, $0))";
	std::vector<cudf::data_type> schema {cudf::data_type{cudf::type_id::TIMESTAMP_MILLISECONDS}};
	std::string out_expression = reinterpret_timestamp(expression, schema);
	std::string expected_str = "Reinterpret(-(CAST(1996-12-01 12:00:01):TIMESTAMP_MILLISECONDS, $0))";

	EXPECT_EQ(out_expression, expected_str);
}

TEST_F(ExpressionUtilsTest, reinterpreting_timestamp_us)
{
	std::string expression = "Reinterpret(-(1996-12-01 12:00:01, $1))";
	std::vector<cudf::data_type> schema {cudf::data_type{cudf::type_id::INT64}, cudf::data_type{cudf::type_id::TIMESTAMP_MICROSECONDS}};
	std::string out_expression = reinterpret_timestamp(expression, schema);
	std::string expected_str = "Reinterpret(-(CAST(1996-12-01 12:00:01):TIMESTAMP_MICROSECONDS, $1))";

	EXPECT_EQ(out_expression, expected_str);
}

TEST_F(ExpressionUtilsTest, reinterpreting_timestamp_ns)
{
	std::string expression = "Reinterpret(-(1996-12-01 12:00:01, $1))";
	std::vector<cudf::data_type> schema {cudf::data_type{cudf::type_id::INT32}, cudf::data_type{cudf::type_id::TIMESTAMP_NANOSECONDS}};
	std::string out_expression = reinterpret_timestamp(expression, schema);

	std::string expected_str = "Reinterpret(-(CAST(1996-12-01 12:00:01):TIMESTAMP, $1))";

	EXPECT_EQ(out_expression, expected_str);
}

TEST_F(ExpressionUtilsTest, modify_multi_column_count_expression_empty)
{
	std::string expression = "";
	std::vector<int> indices;
	std::string out_expr = modify_multi_column_count_expression(expression, indices);
	EXPECT_EQ(out_expr.size(), 0);
	EXPECT_EQ(indices.size(), 0);
}

TEST_F(ExpressionUtilsTest, modify_multi_column_count_expression_count_all)
{
	std::string expression = "COUNT(*)";
	std::vector<int> indices;
	std::string out_expr = modify_multi_column_count_expression(expression, indices);
	EXPECT_EQ(out_expr, expression);
	EXPECT_EQ(indices.size(), 0);
}

TEST_F(ExpressionUtilsTest, modify_multi_column_count_expression_single_col)
{
	std::string expression = "COUNT($2)";
	std::vector<int> indices;
	std::string out_expr = modify_multi_column_count_expression(expression, indices);
	EXPECT_EQ(out_expr, expression);
	EXPECT_EQ(indices.size(), 1);
	EXPECT_EQ(indices[0], 2);
}

TEST_F(ExpressionUtilsTest, modify_multi_column_count_expression_two_cols)
{
	std::string expression = "COUNT($1, $4)";
	std::vector<int> indices;
	std::string out_expr = modify_multi_column_count_expression(expression, indices);
	EXPECT_EQ(out_expr, "+(CAST($1):INTEGER, CAST($4):INTEGER)");
	EXPECT_EQ(indices.size(), 2);
	EXPECT_EQ(indices[0], 1);
	EXPECT_EQ(indices[1], 4);
}

TEST_F(ExpressionUtilsTest, modify_multi_column_count_expression_three_cols)
{
	std::string expression = "COUNT($0, $1, $3)";
	std::vector<int> indices;
	std::string out_expr = modify_multi_column_count_expression(expression, indices);
	EXPECT_EQ(out_expr, "+(+(CAST($0):INTEGER, CAST($1):INTEGER), CAST($3):INTEGER)");
	EXPECT_EQ(indices.size(), 3);
	EXPECT_EQ(indices[0], 0);
	EXPECT_EQ(indices[1], 1);
	EXPECT_EQ(indices[2], 3);
}
