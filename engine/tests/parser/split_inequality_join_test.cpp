#include <gtest/gtest.h>
#include <iostream>
#include "execution_kernels/BatchJoinProcessing.h"

// using ral::batch::split_inequality_join_into_join_and_filter;

struct SplitIneQualityJoinTest : public ::testing::Test {
  SplitIneQualityJoinTest() {

  }

  ~SplitIneQualityJoinTest() {
  }

  void SetUp() override {
      
  }
};

TEST_F(SplitIneQualityJoinTest, case_1) {

  std::string join_statement = "LogicalJoin(condition=[=($3, $0)], joinType=[inner])";
  std::string new_join_statement, filter_statement;
  ral::batch::split_inequality_join_into_join_and_filter(join_statement, new_join_statement, filter_statement);
  std::string expected_new_join_statement = "LogicalJoin(condition=[=($3, $0)], joinType=[inner])";
  std::string expected_filter_statement = "";
  EXPECT_EQ(new_join_statement, expected_new_join_statement);
  EXPECT_EQ(filter_statement, expected_filter_statement);
}

TEST_F(SplitIneQualityJoinTest, case_2) {

  std::string join_statement = "    LogicalJoin(condition=[AND(=($3, $0), >($5, $2))], joinType=[inner])";
  std::string new_join_statement, filter_statement;
  ral::batch::split_inequality_join_into_join_and_filter(join_statement, new_join_statement, filter_statement);
  std::string expected_new_join_statement = "LogicalJoin(condition=[=($3, $0)], joinType=[inner])";
  std::string expected_filter_statement = "LogicalFilter(condition=[>($5, $2)])";
  EXPECT_EQ(new_join_statement, expected_new_join_statement);
  EXPECT_EQ(filter_statement, expected_filter_statement);
}

TEST_F(SplitIneQualityJoinTest, case_3) {

  std::string join_statement = "    LogicalJoin(condition=[AND(=($3, $0), =($5, $2))], joinType=[inner])";
  std::string new_join_statement, filter_statement;
  ral::batch::split_inequality_join_into_join_and_filter(join_statement, new_join_statement, filter_statement);
  std::string expected_new_join_statement = "LogicalJoin(condition=[AND(=($3, $0), =($5, $2))], joinType=[inner])";
  std::string expected_filter_statement = "";
  EXPECT_EQ(new_join_statement, expected_new_join_statement);
  EXPECT_EQ(filter_statement, expected_filter_statement);
}

TEST_F(SplitIneQualityJoinTest, case_4) {

  std::string join_statement = "    LogicalJoin(condition=[AND(=($3, $0), >($5, $2), =($4, $3))], joinType=[inner])";
  std::string new_join_statement, filter_statement;
  ral::batch::split_inequality_join_into_join_and_filter(join_statement, new_join_statement, filter_statement);
  std::string expected_new_join_statement = "LogicalJoin(condition=[AND(=($3, $0), =($4, $3))], joinType=[inner])";
  std::string expected_filter_statement = "LogicalFilter(condition=[>($5, $2)])";
  EXPECT_EQ(new_join_statement, expected_new_join_statement);
  EXPECT_EQ(filter_statement, expected_filter_statement);
}


TEST_F(SplitIneQualityJoinTest, case_5) {

  std::string join_statement = "    LogicalJoin(condition=[AND(<($3, $0), >($5, $2), =($4, $3))], joinType=[inner])";
  std::string new_join_statement, filter_statement;
  ral::batch::split_inequality_join_into_join_and_filter(join_statement, new_join_statement, filter_statement);
  std::string expected_new_join_statement = "LogicalJoin(condition=[=($4, $3)], joinType=[inner])";
  std::string expected_filter_statement = "LogicalFilter(condition=[AND(<($3, $0), >($5, $2))])";
  EXPECT_EQ(new_join_statement, expected_new_join_statement);
  EXPECT_EQ(filter_statement, expected_filter_statement);
}

TEST_F(SplitIneQualityJoinTest, case_6) {

  std::string join_statement = "        LogicalJoin(condition=[AND(=($7, $0), OR(AND($8, $9, $2, $3), AND($8, $9, $4, $5), AND($8, $9, $6, $5)))], joinType=[inner])";
  std::string new_join_statement, filter_statement;
  ral::batch::split_inequality_join_into_join_and_filter(join_statement, new_join_statement, filter_statement);
  std::string expected_new_join_statement = "LogicalJoin(condition=[=($7, $0)], joinType=[inner])";
  std::string expected_filter_statement = "LogicalFilter(condition=[OR(AND($8, $9, $2, $3), AND($8, $9, $4, $5), AND($8, $9, $6, $5))])";
  EXPECT_EQ(new_join_statement, expected_new_join_statement);
  EXPECT_EQ(filter_statement, expected_filter_statement);
}

TEST_F(SplitIneQualityJoinTest, case_7) {
  std::string join_statement = "  LogicalJoin(condition=[AND(IS NOT DISTINCT FROM($0, $3), IS NOT DISTINCT FROM($1, $4))], joinType=[inner])";
  std::string new_join_statement, filter_statement;
  ral::batch::split_inequality_join_into_join_and_filter(join_statement, new_join_statement, filter_statement);
  std::string expected_new_join_statement = "LogicalJoin(condition=[AND(IS_NOT_DISTINCT_FROM($0, $3), IS_NOT_DISTINCT_FROM($1, $4))], joinType=[inner])";
  std::string expected_filter_statement = "";
  EXPECT_EQ(new_join_statement, expected_new_join_statement);
  EXPECT_EQ(filter_statement, expected_filter_statement);
}

TEST_F(SplitIneQualityJoinTest, error_case1) {

  std::string join_statement = "  LogicalJoin(condition=[OR(=($7, $0), AND($8, $9, $2, $3), AND($8, $9, $4, $5), AND($8, $9, $6, $5))], joinType=[inner])";
  std::string new_join_statement, filter_statement;
  try {
    ral::batch::split_inequality_join_into_join_and_filter(join_statement, new_join_statement, filter_statement);
    ASSERT_TRUE(false);  // its not supposed to get here, because its supposed to throw an error
  } catch (...){
    ASSERT_TRUE(true);  // we are expecting an error
  }
}

TEST_F(SplitIneQualityJoinTest, error_case2) {

  std::string join_statement = "  LogicalJoin(condition=[AND(<($7, $0), >($7, $1)], joinType=[inner])";
  std::string new_join_statement, filter_statement;
  try {
    ral::batch::split_inequality_join_into_join_and_filter(join_statement, new_join_statement, filter_statement);
    ASSERT_TRUE(false);  // its not supposed to get here, because its supposed to throw an error
  } catch (...){
    ASSERT_TRUE(true);  // we are expecting an error
  }
}
