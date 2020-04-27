#include "parser/expression_tree.hpp"
#include <gtest/gtest.h>
#include <iostream>

using namespace ral::parser;
struct ExpressionTreeTest : public ::testing::Test {
	ExpressionTreeTest() {}

	~ExpressionTreeTest() {}
};

TEST_F(ExpressionTreeTest, case_1) {
	ral::parser::parse_tree tree;
	tree.build("+($0, CASE(<($0, 5), CASE(<($0, 2), 0, 1), 2))");
	std::string expected =
		"+($0, FIRST_NON_MAGIC(MAGIC_IF_NOT(<($0, 5), FIRST_NON_MAGIC(MAGIC_IF_NOT(<($0, 2), 0), 1)), 2))";
	tree.transform_to_custom_op();
	EXPECT_EQ(tree.rebuildExpression(), expected);
}

TEST_F(ExpressionTreeTest, case_2) {
	ral::parser::parse_tree tree;
	tree.build("CASE(<($0, 5), *($0, 2), $0)");
	std::string expected = "FIRST_NON_MAGIC(MAGIC_IF_NOT(<($0, 5), *($0, 2)), $0)";
	tree.transform_to_custom_op();
	EXPECT_EQ(tree.rebuildExpression(), expected);
}

TEST_F(ExpressionTreeTest, case_3) {
	ral::parser::parse_tree tree;
	tree.build("CASE(<($0, 2), $0, <($0, 5), *($0, 2), +($0, 1))");
	std::string expected =
		"FIRST_NON_MAGIC(MAGIC_IF_NOT(<($0, 2), $0), FIRST_NON_MAGIC(MAGIC_IF_NOT(<($0, 5), *($0, 2)), +($0, 1)))";
	tree.transform_to_custom_op();
	EXPECT_EQ(tree.rebuildExpression(), expected);
}

TEST_F(ExpressionTreeTest, case_4) {
	ral::parser::parse_tree tree;
	tree.build("CASE(IS_NOT_NULL($0), $0, -1)");
	std::string expected = "FIRST_NON_MAGIC(MAGIC_IF_NOT(IS_NOT_NULL($0), $0), -1)";
	tree.transform_to_custom_op();
	EXPECT_EQ(tree.rebuildExpression(), expected);
}

TEST_F(ExpressionTreeTest, case_5) {
	ral::parser::parse_tree tree;
	tree.build("CAST($0):VARCHAR");
	std::string expected = "CAST_VARCHAR($0)";
	tree.transform_to_custom_op();
	EXPECT_EQ(tree.rebuildExpression(), expected);
}

TEST_F(ExpressionTreeTest, case_6) {
	ral::parser::parse_tree tree;
	tree.build("CAST(+($0, CAST($1):BIGINT)):VARCHAR");
	std::string expected = "CAST_VARCHAR(+($0, CAST_BIGINT($1)))";
	tree.transform_to_custom_op();
	EXPECT_EQ(tree.rebuildExpression(), expected);
}

TEST_F(ExpressionTreeTest, case_7) {
	ral::parser::parse_tree tree;
	tree.build("CASE(<(CAST($0):INTEGER, 5), CAST($1):BIGINT, CAST($2):BIGINT)");
	std::string expected = "FIRST_NON_MAGIC(MAGIC_IF_NOT(<(CAST_INTEGER($0), 5), CAST_BIGINT($1)), CAST_BIGINT($2))";
	tree.transform_to_custom_op();
	EXPECT_EQ(tree.rebuildExpression(), expected);
}

TEST_F(ExpressionTreeTest, case_8) {
	ral::parser::parse_tree tree;
	tree.build("CAST(CAST($0):BIGINT):VARCHAR");
	std::string expected = "CAST_VARCHAR(CAST_BIGINT($0))";
	tree.transform_to_custom_op();
	EXPECT_EQ(tree.rebuildExpression(), expected);
}

TEST_F(ExpressionTreeTest, case_9) {
	ral::parser::parse_tree tree;
	tree.build("SUBSTRING($0, 1)");
	std::string expected = "SUBSTRING($0, '1')";
	tree.transform_to_custom_op();
	EXPECT_EQ(tree.rebuildExpression(), expected);
}

TEST_F(ExpressionTreeTest, case_10) {
	ral::parser::parse_tree tree;
	tree.build("SUBSTRING($0, 1, 5)");
	std::string expected = "SUBSTRING($0, '1:5')";
	tree.transform_to_custom_op();
	EXPECT_EQ(tree.rebuildExpression(), expected);
}
