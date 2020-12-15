#include <gtest/gtest.h>
#include "parser/expression_tree.hpp"
#include "skip_data/SkipDataProcessor.h"

using namespace ral;
using namespace skip_data;
struct ExpressionTreeTest : public ::testing::Test {
  ExpressionTreeTest() {

  }

  ~ExpressionTreeTest() {
  }

  void SetUp() override {

  }

  void process(std::string prefix, std::string expected, bool /*valid_expr*/ = true) {
    ral::parser::parse_tree tree;
    tree.build(prefix);
      std::cout << "before:\n";
      tree.print();
      ral::skip_data::apply_skip_data_rules(tree);
      std::cout << "after:\n";
      tree.print();
      auto solution =  tree.prefix();
      std::cout << "solution:\n";
      std::cout << solution << "\n";
//      auto rebuilt =  tree.rebuildExpression();
//      std::cout << "rebuilt:\n";
//      std::cout << rebuilt << "\n";
      EXPECT_EQ(solution, expected);
  }
};

TEST_F(ExpressionTreeTest, equal) {
  std::string prefix = "=($0, $1)";
  std::string expected = "AND <= $0 $3 >= $1 $2";
  process(prefix, expected);
}

TEST_F(ExpressionTreeTest, less) {
  std::string prefix = "<($0, $1)";
  std::string expected = "< $0 $3";
  process(prefix, expected);
}
TEST_F(ExpressionTreeTest, less_eq) {
  std::string prefix = "<=($0, $1)";
  std::string expected = "<= $0 $3";
  process(prefix, expected);
}

TEST_F(ExpressionTreeTest, greater) {
  std::string prefix = ">($0, $1)";
  std::string expected = "> $1 $2";
  process(prefix, expected);
}
TEST_F(ExpressionTreeTest, greater_eq) {
  std::string prefix = ">=($0, $1)";
  std::string expected = ">= $1 $2";
  process(prefix, expected);
}
TEST_F(ExpressionTreeTest, add) {
  std::string prefix = "+($0, $1)";
  std::string expected = "&&& + $0 $2 + $1 $3";
  process(prefix, expected);
}
TEST_F(ExpressionTreeTest, sub) {
  std::string prefix = "-($0, $1)";
  std::string expected = "&&& - $0 $2 - $1 $3";
  process(prefix, expected);
}
TEST_F(ExpressionTreeTest, expr_test_1) {
  std::string prefix = "=(+($0, $1), 123)";
  std::string expected = "AND <= + $0 $2 123 >= + $1 $3 123";
  process(prefix, expected);
}

TEST_F(ExpressionTreeTest, expr_test_2) {
  std::string prefix = "OR(AND(AND(>($0, 100), =(+($0, $1), 123)), <($1, 10)), =($0, 500))";
  std::string expected = "OR AND AND > $1 100 AND <= + $0 $2 123 >= + $1 $3 123 < $2 10 AND <= $0 500 >= $1 500";
  process(prefix, expected);
}

TEST_F(ExpressionTreeTest, expr_test_3) {
  std::string prefix = "AND(=(COS(+($0, $1)), 123), =($0, $1))";
  std::string expected = "AND <= $0 $3 >= $1 $2";
  process(prefix, expected);
}

TEST_F(ExpressionTreeTest, expr_test_4) {
  std::string prefix = "AND(AND(AND(>($0, 100), =(*($0, $1), 123)), <($1, 10)), /($0, 500))";
  std::string expected = "AND > $1 100 < $2 10";
  process(prefix, expected);
}

TEST_F(ExpressionTreeTest, expr_test_5) {
  std::string prefix = "OR(AND(AND(>($0, 100), =(*($0, $1), 123)), <($1, 10)), /($0, 500))";
  std::string expected = "NONE";
  process(prefix, expected);
}

// TEST_F(ExpressionTreeTest, expr_test_6) {
//   std::string prefix = "AND = SQRT + $0 $1 123 = $0 $1";
//   std::string expected = "";
//   bool valid_expr = false; // because SQRT is not supported
//   process(prefix, expected, valid_expr);
// }

// TEST_F(ExpressionTreeTest, expr_test_7) {
//   std::string prefix = "AND = ACOS + $0 $1 123 LEAST $0 111";
//   std::string expected = "";
//   bool valid_expr = false; // because LEAST is not supported
//   process(prefix, expected, valid_expr);
// }

//TEST_F(ExpressionTreeTest, expr_test_8) {
//  std::string prefix = "OR(>($0, 100), =(+, $0))";
//  std::string expected = "";
//  bool valid_expr = false;
//  process(prefix, expected, valid_expr);
//}

TEST_F(ExpressionTreeTest, expr_test_9) {
  std::string prefix = "OR(>($0, 100), =($0, 500))";
  std::string expected = "OR > $1 100 AND <= $0 500 >= $1 500";
  bool valid_expr = true;
  process(prefix, expected, valid_expr);
}

TEST_F(ExpressionTreeTest, expr_test_10) {
  std::string prefix = "IS NULL($0)";
  std::string expected = "NONE";
  bool valid_expr = true;
  process(prefix, expected, valid_expr);
}

TEST_F(ExpressionTreeTest, expr_test_11) {
  std::string prefix = "AND(IS NOT NULL($0), =($1, 500))";
  std::string expected = "AND <= $2 500 >= $3 500";
  bool valid_expr = true;
  process(prefix, expected, valid_expr);
}

TEST_F(ExpressionTreeTest, expr_test_12) {
  std::string prefix = "OR(IS NOT NULL($0), =($1, 500))";
  std::string expected = "NONE";
  bool valid_expr = true;
  process(prefix, expected, valid_expr);
}

TEST_F(ExpressionTreeTest, drop_test1) {
  std::string prefix = "OR(AND(AND(>($0, 100), =(+($0, $1), 123)), <($1, 10)), =($0, 500))";
  std::string expected = "OR > $1 100 AND <= $0 500 >= $1 500";
  ral::parser::parse_tree tree;
  tree.build(prefix);
    std::cout << "before:\n";
    tree.print();
    ral::skip_data::drop_value(tree, "$1");
    std::cout << "after drop $1:\n";

    tree.print();

    std::string solution =  tree.prefix();
    std::cout << "after drop solution:\n";
    std::cout << solution << "\n";
    ral::skip_data::apply_skip_data_rules(tree);
    std::cout << "after skip_data rules:\n";
    tree.print();
    solution =  tree.prefix();
    std::cout << "solution:\n";
    std::cout << solution << "\n";
    EXPECT_EQ(solution, expected);

}

TEST_F(ExpressionTreeTest, drop_test2) {
  std::string prefix = "OR(AND(AND(>($2, 100), =(+($0, $1), 123)), <($1, 10)), =($0, 500))";
  std::string expected = "OR > $5 100 AND <= $0 500 >= $1 500";
  ral::parser::parse_tree tree;

  tree.build(prefix);
    std::cout << "before:\n";
    tree.print();
    ral::skip_data::drop_value(tree, "$1");
    std::cout << "after drop $1:\n";

    tree.print();

    std::string solution =  tree.prefix();
    std::cout << "after drop solution:\n";
    std::cout << solution << "\n";
    ral::skip_data::apply_skip_data_rules(tree);
    std::cout << "after skip_data rules:\n";
    tree.print();
    solution =  tree.prefix();
    std::cout << "solution:\n";
    std::cout << solution << "\n";
    EXPECT_EQ(solution, expected);
}

TEST_F(ExpressionTreeTest, drop_test3) {
  std::string prefix = "AND(=(+($0, $1), 123), =($0, $1))";
  std::string expected = "NONE";
   ral::parser::parse_tree tree;
   tree.build(prefix);
    std::cout << "before:\n";
    tree.print();
    ral::skip_data::drop_value(tree, "$0");
    ral::skip_data::drop_value(tree, "$1");
    std::cout << "after drop $1:\n";

    tree.print();

    std::string solution =  tree.prefix();
    std::cout << "after drop solution:\n";
    std::cout << solution << "\n";
    ral::skip_data::apply_skip_data_rules(tree);
    std::cout << "after skip_data rules:\n";
    tree.print();
    solution =  tree.prefix();
    std::cout << "solution:\n";
    std::cout << solution << "\n";
    EXPECT_EQ(solution, expected);

}
