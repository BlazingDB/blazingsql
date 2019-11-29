
#include <cstdlib>
#include <iostream>
#include <string>
#include <vector>

#include <CalciteExpressionParsing.h>
#include <CalciteInterpreter.h>
#include <DataFrame.h>
#include <blazingdb/io/Util/StringUtil.h>
#include <gtest/gtest.h>
#include <GDFColumn.cuh>
#include <GDFCounter.cuh>
#include <Utils.cuh>

#include "gdf/library/api.h"
using namespace gdf::library;
#include "../query_test.h"

struct EvaluateQueryTest : public query_test {
  struct InputTestItem {
    std::string query;
    std::string logicalPlan;
    gdf::library::TableGroup tableGroup;
    gdf::library::Table resultTable;
  };
  void SetUp() {
    rmmInitialize(nullptr);
  }
  
  void CHECK_RESULT(gdf::library::Table& computed_solution,
                    gdf::library::Table& reference_solution) {
    computed_solution.print(std::cout);
    reference_solution.print(std::cout);

    for (size_t index = 0; index < reference_solution.size(); index++) {
      const auto& reference_column = reference_solution[index];
      const auto& computed_column = computed_solution[index];
      auto a = reference_column.to_string();
      auto b = computed_column.to_string();
      EXPECT_EQ(a, b);
    }
  }
};

// AUTO GENERATED UNIT TESTS
// MODIFIED BY HAND TO FIX IT 
TEST_F(EvaluateQueryTest, TEST_00) {
  auto input = InputTestItem{
      .query =
          "select n1.n_nationkey as n1key, n2.n_nationkey as n2key, "
          "n1.n_nationkey + n2.n_nationkey from main.nation as n1 full outer "
          "join main.nation as n2 on n1.n_nationkey = n2.n_nationkey + 6 order by n1key",
      .logicalPlan =
          "LogicalSort(sort0=[$0], dir0=[ASC])\n  "
          "LogicalProject(n1key=[$0], n2key=[$4], EXPR$2=[+($0, $4)])\n    "
          "LogicalProject(n_nationkey=[$0], n_name=[$1], n_regionkey=[$2], "
          "n_comment=[$3], n_nationkey0=[$4], n_name0=[$5], n_regionkey0=[$6], "
          "n_comment0=[$7])\n      LogicalJoin(condition=[=($0, $8)], "
          "joinType=[full])\n        LogicalTableScan(table=[[main, "
          "nation]])\n        LogicalProject(n_nationkey=[$0], n_name=[$1], "
          "n_regionkey=[$2], n_comment=[$3], $f4=[+($0, 6)])\n          "
          "LogicalTableScan(table=[[main, nation]])",
      .tableGroup =
          LiteralTableGroupBuilder{
              {"main.nation",
               {{"n_nationkey",
                 Literals<GDF_INT32>{0,  1,  2,  3,  4,  5,  6,  7,  8,
                                     9,  10, 11, 12, 13, 14, 15, 16, 17,
                                     18, 19, 20, 21, 22, 23, 24}},
                {"n_name",
                 Literals<GDF_INT64>{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                                     0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}},
                {"n_regionkey",
                 Literals<GDF_INT32>{0, 1, 1, 1, 4, 0, 3, 3, 2, 2, 4, 4, 2,
                                     4, 0, 0, 0, 1, 2, 3, 4, 2, 3, 3, 1}},
                {"n_comment",
                 Literals<GDF_INT64>{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                                     0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}}}}}
              .Build(),
      .resultTable =
          LiteralTableBuilder{"ResultSet",
                              {{"n1key",
                                Literals<GDF_INT32>{
                                    Literals<GDF_INT32>::vector{0,  1,  2,  3,  4,  5,  6,  7,  8,  9,  10, 11, 12, 13, 14, 15, 16,
                                    17, 18, 19, 20, 21, 22, 23, 24, -1, -1, -1,
                                    -1, -1, -1 },
                                    Literals<GDF_INT32>::bool_vector{1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
                                    1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 0, 0, 0, 0, 0, 0 }}},
                               {"n2key",
                                Literals<GDF_INT32>{
                                    Literals<GDF_INT32>::vector{-1, -1, -1, -1, -1, -1, 0,  1,  2,  3,  4,  5,  6,  7,  8,  9,  10,
                                    11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21,
                                    22, 23, 24},
                                    Literals<GDF_INT32>::bool_vector{0, 0, 0, 0, 0, 0, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
                                    1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1 }}},
                               {"GDF_FLOAT64",
                                Literals<GDF_INT32>{
                                    Literals<GDF_INT32>::vector{ -1, -1, -1, -1, -1, -1,  6,  8,  10, 12, 14, 16, 18, 20, 22, 24, 26,
                                    28, 30, 32, 34, 36, 38, 40, 42, -1, -1, -1, -1, -1, -1},
                                    Literals<GDF_INT32>::bool_vector{ 0, 0, 0, 0, 0, 0, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
                                    1, 1, 1, 1, 1, 1, 0, 0, 0, 0, 0, 0 }}}
                                }}
              .Build()};
  auto logical_plan = input.logicalPlan;
  auto input_tables = input.tableGroup.ToBlazingFrame();
  auto table_names = input.tableGroup.table_names();
  auto column_names = input.tableGroup.column_names();
  std::vector<gdf_column_cpp> outputs;
  gdf_error err = evaluate_query(input_tables, table_names, column_names,
                                 logical_plan, outputs);
  EXPECT_TRUE(err == GDF_SUCCESS);
  auto output_table =
      GdfColumnCppsTableBuilder{"output_table", outputs}.Build();
  CHECK_RESULT(output_table, input.resultTable);
}
TEST_F(EvaluateQueryTest, TEST_01) {
  auto input = InputTestItem{
      .query =
          "select n1.n_nationkey as n1key, n2.n_nationkey as n2key, "
          "n1.n_nationkey + n2.n_nationkey from main.nation as n1 full outer "
          "join main.nation as n2 on n1.n_nationkey = n2.n_nationkey + 6 where "
          "n1.n_nationkey < 10 order by n1key",
      .logicalPlan =
          "LogicalSort(sort0=[$0], dir0=[ASC])\n"
          "  LogicalProject(n1key=[$0], n2key=[$1], EXPR$2=[+($0, $1)])\n"
          "    LogicalJoin(condition=[=($0, $2)], joinType=[left])\n"
          "      LogicalProject(n_nationkey=[$0])\n"
          "        LogicalFilter(condition=[<($0, 10)])\n"
          "          LogicalTableScan(table=[[main, nation]])\n"
          "      LogicalProject(n_nationkey=[$0], $f4=[$4])\n"
          "        LogicalProject(n_nationkey=[$0], n_name=[$1], n_regionkey=[$2], n_comment=[$3], $f4=[+($0, 6)])\n"
          "          LogicalTableScan(table=[[main, nation]])",          
      .tableGroup =
          LiteralTableGroupBuilder{
              {"main.nation",
               {{"n_nationkey",
                 Literals<GDF_INT32>{0,  1,  2,  3,  4,  5,  6,  7,  8,
                                     9,  10, 11, 12, 13, 14, 15, 16, 17,
                                     18, 19, 20, 21, 22, 23, 24}},
                {"n_name",
                 Literals<GDF_INT64>{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                                     0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}},
                {"n_regionkey",
                 Literals<GDF_INT32>{0, 1, 1, 1, 4, 0, 3, 3, 2, 2, 4, 4, 2,
                                     4, 0, 0, 0, 1, 2, 3, 4, 2, 3, 3, 1}},
                {"n_comment",
                 Literals<GDF_INT64>{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                                     0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}}}}}
              .Build(),
        .resultTable =
          LiteralTableBuilder{"ResultSet",
                              {{"n1key",
                                Literals<GDF_INT32>{
                                    Literals<GDF_INT32>::vector{0, 1, 2, 3, 4, 5, 6, 7, 8, 9 },
                                    Literals<GDF_INT32>::bool_vector{1, 1, 1, 1, 1, 1, 1, 1, 1, 1 }}},
                               {"n2key",
                                Literals<GDF_INT32>{
                                    Literals<GDF_INT32>::vector{-1, -1, -1, -1, -1, -1, 0, 1, 2, 3 },
                                    Literals<GDF_INT32>::bool_vector{0, 0, 0, 0, 0, 0, 1, 1, 1, 1 }}},
                               {"GDF_FLOAT64",
                                Literals<GDF_INT32>{
                                    Literals<GDF_INT32>::vector{ -1, -1, -1, -1, -1, -1, 6, 8, 10, 12},
                                    Literals<GDF_INT32>::bool_vector{0, 0, 0, 0, 0, 0, 1, 1, 1, 1 }}}
                                }}
              .Build()};
  auto logical_plan = input.logicalPlan;
  auto input_tables = input.tableGroup.ToBlazingFrame();
  auto table_names = input.tableGroup.table_names();
  auto column_names = input.tableGroup.column_names();
  std::vector<gdf_column_cpp> outputs;
  gdf_error err = evaluate_query(input_tables, table_names, column_names,
                                 logical_plan, outputs);
  EXPECT_TRUE(err == GDF_SUCCESS);
  auto output_table =
      GdfColumnCppsTableBuilder{"output_table", outputs}.Build();
  CHECK_RESULT(output_table, input.resultTable);
}
TEST_F(EvaluateQueryTest, TEST_02) {
  auto input = InputTestItem{
      .query =
          "select n1.n_nationkey as n1key, n2.n_nationkey as n2key, "
          "n1.n_nationkey + n2.n_nationkey from main.nation as n1 full outer "
          "join main.nation as n2 on n1.n_nationkey = n2.n_nationkey + 6 where "
          "n1.n_nationkey < 10 and n1.n_nationkey > 5  order by n1key",
      .logicalPlan =
          "LogicalSort(sort0=[$0], dir0=[ASC])\n  "
          "LogicalProject(n1key=[$0], n2key=[$4], EXPR$2=[+($0, $4)])\n    "
          "LogicalFilter(condition=[AND(<($0, 10), >($0, 5))])\n      "
          "LogicalProject(n_nationkey=[$0], n_name=[$1], n_regionkey=[$2], "
          "n_comment=[$3], n_nationkey0=[$4], n_name0=[$5], n_regionkey0=[$6], "
          "n_comment0=[$7])\n        LogicalJoin(condition=[=($0, $8)], "
          "joinType=[full])\n          LogicalTableScan(table=[[main, "
          "nation]])\n          LogicalProject(n_nationkey=[$0], n_name=[$1], "
          "n_regionkey=[$2], n_comment=[$3], $f4=[+($0, 6)])\n            "
          "LogicalTableScan(table=[[main, nation]])",
      .tableGroup =
          LiteralTableGroupBuilder{
              {"main.nation",
               {{"n_nationkey",
                 Literals<GDF_INT32>{0,  1,  2,  3,  4,  5,  6,  7,  8,
                                     9,  10, 11, 12, 13, 14, 15, 16, 17,
                                     18, 19, 20, 21, 22, 23, 24}},
                {"n_name",
                 Literals<GDF_INT64>{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                                     0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}},
                {"n_regionkey",
                 Literals<GDF_INT32>{0, 1, 1, 1, 4, 0, 3, 3, 2, 2, 4, 4, 2,
                                     4, 0, 0, 0, 1, 2, 3, 4, 2, 3, 3, 1}},
                {"n_comment",
                 Literals<GDF_INT64>{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                                     0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}}}}}
              .Build(),
      .resultTable =
          LiteralTableBuilder{
              "ResultSet",
              {{"GDF_INT64", Literals<GDF_INT64>{6, 7, 8, 9}},
               {"GDF_INT64", Literals<GDF_INT64>{0, 1, 2, 3}},
               {"GDF_INT64", Literals<GDF_INT64>{6, 8, 10, 12}}}}
              .Build()};
  auto logical_plan = input.logicalPlan;
  auto input_tables = input.tableGroup.ToBlazingFrame();
  auto table_names = input.tableGroup.table_names();
  auto column_names = input.tableGroup.column_names();
  std::vector<gdf_column_cpp> outputs;
  gdf_error err = evaluate_query(input_tables, table_names, column_names,
                                 logical_plan, outputs);
  EXPECT_TRUE(err == GDF_SUCCESS);
  auto output_table =
      GdfColumnCppsTableBuilder{"output_table", outputs}.Build();
  CHECK_RESULT(output_table, input.resultTable);
}
