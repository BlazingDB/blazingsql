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

	std::vector<size_t> expected = {0}; // We want to load only one column, the first by default
	std::vector<size_t> result = get_projections(query_part);

	EXPECT_EQ(result, expected);
}
