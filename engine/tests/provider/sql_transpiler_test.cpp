/*
 * Copyright 2021 Percy Camilo Triveño Aucahuasi <percy.camilo.ta@gmail.com>
 */

#include <numeric>
#include <gtest/gtest.h>

#include "io/data_provider/sql/compatibility/SQLTranspiler.h"
#include "io/DataType.h"

using namespace ral::io;
using namespace ral::parser;

struct SQLTranspilerTestParam {
  SQLTranspilerTestParam(const std::string &queryPart,
    const std::string &expected,
    const std::map<operator_type, sql_tools::operator_info> &operators = {},
    DataType transformer_type = DataType::UNDEFINED):
      transformer_type(transformer_type), queryPart(queryPart),
      operators(operators), expected(expected) {}

  DataType transformer_type; // when UNDEFINED use default_predicate_transformer
  std::string queryPart;
  std::vector<std::string> column_names;
  std::vector<int> column_indices;
  std::map<operator_type, sql_tools::operator_info> operators;
  std::string expected;
};

struct SQLTranspilerTest : public testing::TestWithParam<SQLTranspilerTestParam> {
	void SetUp() override {
    auto get_all_lineitem_col_idxs = [] (size_t column_count) -> std::vector<int> {
        std::vector<int> ret(column_count);
        std::iota(ret.begin(), ret.end(), 0);
        return ret;
    };

    auto get_ops_without_pars = [] () -> std::map<operator_type, ral::io::sql_tools::operator_info> {
        auto ret = ral::io::sql_tools::get_default_operators();
        for (auto& [op, op_info] : ret) {
            op_info.parentheses_wrap = false;
        }
        ral::io::sql_tools::operator_info oi;
        oi.parentheses_wrap = false;
        ret[operator_type::BLZ_LOGICAL_AND] = oi;
        ret[operator_type::BLZ_LOGICAL_OR] = oi;
        ret[operator_type::BLZ_ADD] = oi;
        ret[operator_type::BLZ_SUB] = oi;
        ret[operator_type::BLZ_MUL] = oi;
        ret[operator_type::BLZ_DIV] = oi;
        ret[operator_type::BLZ_LESS] = oi;
        ret[operator_type::BLZ_LESS_EQUAL] = oi;
        ret[operator_type::BLZ_GREATER] = oi;
        ret[operator_type::BLZ_GREATER_EQUAL] = oi;
        return ret;
    };

    std::vector<std::string> lineitem_cols = {
      "l_orderkey", "l_partkey", "l_suppkey", "l_linenumber", "l_quantity",
      "l_extendedprice", "l_discount", "l_tax", "l_returnflag", "l_linestatus",
      "l_shipdate", "l_commitdate", "l_receiptdate", "l_shipinstruct",
      "l_shipmode", "l_comment"};
    auto all_lineitem_col_idxs = get_all_lineitem_col_idxs(lineitem_cols.size());
    auto ops_without_pars = get_ops_without_pars();

    auto p = GetParam();
    this->transformer_type = p.transformer_type;
    this->queryPart = p.queryPart;
    this->column_names = p.column_names.empty()? std::move(lineitem_cols) : p.column_names;
    this->column_indices = p.column_indices.empty()? std::move(all_lineitem_col_idxs) : p.column_indices;
    this->operators = p.operators.empty()? std::move(ops_without_pars) : p.operators;
    this->expected = p.expected;
  }

	void TearDown() override {}

protected:
  DataType transformer_type;
  std::string queryPart;
  std::vector<std::string> column_names;
  std::vector<int> column_indices;
  std::map<operator_type, sql_tools::operator_info> operators;
  std::string expected;
};

const std::vector<SQLTranspilerTestParam> default_check_entries = {
  SQLTranspilerTestParam(
    "BindableTableScan(table=[[main, lineitem]], filters=[[<(+($5, $6), 100)]])",
    "l_extendedprice + l_discount < 100"
  ),
  SQLTranspilerTestParam(
    "BindableTableScan(table=[[main, lineitem]], filters=[[<(+($5, $6), 100)]])",
    "((l_extendedprice + l_discount) < 100)",
    sql_tools::get_default_operators()
  ),
  SQLTranspilerTestParam(
    "BindableTableScan(table=[[main, lineitem_MYSQL]], filters=[[AND(>=($10, 1995-01-01), <=($11, 1996-12-31))]], projects=[[0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11]], aliases=[[l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus, l_shipdate, l_commitdate]])",
    "((l_shipdate >= \"1995-01-01\") AND (l_commitdate <= \"1996-12-31\"))",
    sql_tools::get_default_operators()
  ),
  SQLTranspilerTestParam(
    "BindableTableScan(table=[[main, lineitem_MYSQL]], filters=[[AND(<($6, $5), IS NOT NULL($0))]], projects=[[0, 1, 2, 3, 4, 5, 6]], aliases=[[l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity, l_extendedprice, l_discount]])",
    "l_discount < l_extendedprice AND l_orderkey IS NOT NULL"
  )
};

TEST_P(SQLTranspilerTest, default_check) {
  // Test setup
  std::unique_ptr<node_transformer> predicate_transformer = nullptr;
  switch (this->transformer_type) {
    case DataType::MYSQL: {} break;
    case DataType::POSTGRESQL: {} break;
    case DataType::SQLITE: {} break;
    case DataType::UNDEFINED: {
      predicate_transformer = std::unique_ptr<node_transformer>(
        new sql_tools::default_predicate_transformer(this->column_indices,
                                                     this->column_names,
                                                     this->operators));
    } break;
  }

  // Test subject
  auto result = sql_tools::transpile_predicate(this->queryPart, predicate_transformer.get());

  // Test assert
  EXPECT_EQ(result, this->expected);
}

INSTANTIATE_TEST_SUITE_P(SQLTranspilerTestCase, SQLTranspilerTest, testing::ValuesIn(default_check_entries));
