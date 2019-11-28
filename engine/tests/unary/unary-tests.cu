
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
#include "../query_test.h"
using namespace gdf::library;

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
TEST_F(EvaluateQueryTest, TEST_00) {
  auto input =
      InputTestItem{
          .query =
              "select sin(value_), cos(value_), tan(value_), asin(sin_value), "
              "acos(cos_value), atan(tan_value)  from main.emps",
          .logicalPlan =
              "LogicalProject(EXPR$0=[SIN($1)], EXPR$1=[COS($1)], "
              "EXPR$2=[TAN($1)], EXPR$3=[ASIN($2)], EXPR$4=[ACOS($3)], "
              "EXPR$5=[ATAN($4)])\n  LogicalTableScan(table=[[main, emps]])",
          .tableGroup =
              LiteralTableGroupBuilder{
                  {"main.emps",
               {{"id", Literals<GDF_INT32>{1,  2,  3,  4,  5,  6,  7,  8,  9,
                                           10, 11, 12, 13, 14, 15, 16, 17, 18,
                                           19, 20, 21, 22, 23, 24, 25}},
                    {"value_", Literals<GDF_FLOAT64>{0,
                                                     0.4188790205,
                                                     0.837758041,
                                                     1.2566370614,
                                                     1.6755160819,
                                                     2.0943951024,
                                                     2.5132741229,
                                                     2.9321531434,
                                                     3.3510321638,
                                                     3.7699111843,
                                                     4.1887902048,
                                                     4.6076692253,
                                                     5.0265482457,
                                                     5.4454272662,
                                                     5.8643062867,
                                                     187.962235974,
                                                     208.4706077733,
                                                     379.980656048,
                                                     450.4093141932,
                                                     253.964754701,
                                                     -5.4169921017,
                                                     -78.5231677362,
                                                     -136.9926093898,
                                                     -376.069367015,
                                                     -445.3764402335}},
                    {"sin_value", Literals<GDF_FLOAT64>{0,
                                                        0.4067366431,
                                                        0.7431448255,
                                                        0.9510565163,
                                                        0.9945218954,
                                                        0.8660254038,
                                                        0.5877852523,
                                                        0.2079116908,
                                                        -0.2079116908,
                                                        -0.5877852523,
                                                        -0.8660254038,
                                                        -0.9945218954,
                                                        -0.9510565163,
                                                        -0.7431448255,
                                                        -0.4067366431,
                                                        -0.5083978605,
                                                        0.9024798978,
                                                        0.151469776,
                                                        -0.9174268447,
                                                        0.4831511334,
                                                        0.7618686826,
                                                        -0.0166478344,
                                                        0.9449583777,
                                                        0.7966614422,
                                                        0.6666584081}},
                    {"cos_value", Literals<GDF_FLOAT64>{1,
                                                        0.9135454576,
                                                        0.6691306064,
                                                        0.3090169944,
                                                        -0.1045284633,
                                                        -0.5,
                                                        -0.8090169944,
                                                        -0.9781476007,
                                                        -0.9781476007,
                                                        -0.8090169944,
                                                        -0.5,
                                                        -0.1045284633,
                                                        0.3090169944,
                                                        0.6691306064,
                                                        0.9135454576,
                                                        0.8611222999,
                                                        0.4307319747,
                                                        -0.9884618895,
                                                        -0.3979044919,
                                                        -0.875536968,
                                                        0.6477315111,
                                                        -0.9998614152,
                                                        0.3271905629,
                                                        0.604425799,
                                                        0.7453633791}},
                    {"tan_value", Literals<GDF_FLOAT64>{0,
                                                        0.4452286853,
                                                        1.1106125148,
                                                        3.0776835372,
                                                        -9.5143644542,
                                                        -1.7320508076,
                                                        -0.726542528,
                                                        -0.2125565617,
                                                        0.2125565617,
                                                        0.726542528,
                                                        1.7320508076,
                                                        9.5143644542,
                                                        -3.0776835372,
                                                        -1.1106125148,
                                                        -0.4452286853,
                                                        -0.5903898442,
                                                        2.0952238303,
                                                        -0.1532378513,
                                                        2.3056458608,
                                                        -0.5518340756,
                                                        1.1762106204,
                                                        0.0166501419,
                                                        2.8880979003,
                                                        1.3180467207,
                                                        0.8944072472}}}}}
                  .Build(),
          .resultTable =
              LiteralTableBuilder{
                  "ResultSet",
                  {{"GDF_FLOAT64", Literals<GDF_FLOAT64>{0,
                                                         0.4067366431,
                                                         0.7431448255,
                                                         0.9510565163,
                                                         0.9945218954,
                                                         0.8660254038,
                                                         0.5877852523,
                                                         0.2079116908,
                                                         -0.2079116908,
                                                         -0.5877852523,
                                                         -0.8660254038,
                                                         -0.9945218954,
                                                         -0.9510565163,
                                                         -0.7431448255,
                                                         -0.4067366431,
                                                         -0.5083978605,
                                                         0.9024798978,
                                                         0.151469776,
                                                         -0.9174268447,
                                                         0.4831511334,
                                                         0.7618686826,
                                                         -0.0166478344,
                                                         0.9449583777,
                                                         0.7966614422,
                                                         0.6666584081}},
                   {"GDF_FLOAT64", Literals<GDF_FLOAT64>{1,
                                                         0.9135454576,
                                                         0.6691306064,
                                                         0.3090169944,
                                                         -0.1045284633,
                                                         -0.5,
                                                         -0.8090169944,
                                                         -0.9781476007,
                                                         -0.9781476007,
                                                         -0.8090169944,
                                                         -0.5,
                                                         -0.1045284633,
                                                         0.3090169944,
                                                         0.6691306064,
                                                         0.9135454576,
                                                         0.8611222999,
                                                         0.4307319747,
                                                         -0.9884618895,
                                                         -0.3979044919,
                                                         -0.875536968,
                                                         0.6477315111,
                                                         -0.9998614152,
                                                         0.3271905629,
                                                         0.604425799,
                                                         0.7453633791}},
                   {"GDF_FLOAT64", Literals<GDF_FLOAT64>{0,
                                                         0.4452286853,
                                                         1.1106125148,
                                                         3.0776835372,
                                                         -9.5143644542,
                                                         -1.7320508076,
                                                         -0.726542528,
                                                         -0.2125565617,
                                                         0.2125565617,
                                                         0.726542528,
                                                         1.7320508076,
                                                         9.5143644542,
                                                         -3.0776835372,
                                                         -1.1106125148,
                                                         -0.4452286853,
                                                         -0.5903898442,
                                                         2.0952238303,
                                                         -0.1532378513,
                                                         2.3056458608,
                                                         -0.5518340756,
                                                         1.1762106204,
                                                         0.0166501419,
                                                         2.8880979003,
                                                         1.3180467207,
                                                         0.8944072472}},
                   {"GDF_FLOAT64", Literals<GDF_FLOAT64>{0,
                                                         0.4188790205,
                                                         0.837758041,
                                                         1.2566370614,
                                                         1.4660765717,
                                                         1.0471975512,
                                                         0.6283185307,
                                                         0.2094395102,
                                                         -0.2094395102,
                                                         -0.6283185307,
                                                         -1.0471975512,
                                                         -1.4660765717,
                                                         -1.2566370614,
                                                         -0.837758041,
                                                         -0.4188790205,
                                                         -0.5333232413,
                                                         1.1254926363,
                                                         0.1520550363,
                                                         -1.1615647299,
                                                         0.5042502398,
                                                         0.8661932055,
                                                         -0.0166486035,
                                                         1.2374673682,
                                                         0.9217514158,
                                                         0.7297165762}},
                   {"GDF_FLOAT64", Literals<GDF_FLOAT64>{0,
                                                         0.4188790205,
                                                         0.837758041,
                                                         1.2566370614,
                                                         1.6755160819,
                                                         2.0943951024,
                                                         2.5132741229,
                                                         2.9321531434,
                                                         2.9321531434,
                                                         2.5132741229,
                                                         2.0943951024,
                                                         1.6755160819,
                                                         1.2566370614,
                                                         0.837758041,
                                                         0.4188790205,
                                                         0.5333232413,
                                                         1.1254926363,
                                                         2.9895376172,
                                                         1.9800279237,
                                                         2.6373424138,
                                                         0.8661932055,
                                                         3.1249440501,
                                                         1.2374673682,
                                                         0.9217514158,
                                                         0.7297165762}},
                   {"GDF_FLOAT64", Literals<GDF_FLOAT64>{0,
                                                         0.4188790205,
                                                         0.837758041,
                                                         1.2566370614,
                                                         -1.4660765717,
                                                         -1.0471975512,
                                                         -0.6283185307,
                                                         -0.2094395102,
                                                         0.2094395102,
                                                         0.6283185307,
                                                         1.0471975512,
                                                         1.4660765717,
                                                         -1.2566370614,
                                                         -0.837758041,
                                                         -0.4188790205,
                                                         -0.5333232413,
                                                         1.1254926363,
                                                         -0.1520550363,
                                                         1.1615647299,
                                                         -0.5042502398,
                                                         0.8661932055,
                                                         0.0166486035,
                                                         1.2374673682,
                                                         0.9217514158,
                                                         0.7297165762}}}}
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
      .query = "select ln(value_) from main.emps",
      .logicalPlan =
          "LogicalProject(EXPR$0=[LN($1)])\n  "
          "LogicalTableScan(table=[[main, emps]])",
      .tableGroup =
          LiteralTableGroupBuilder{
              {"main.emps",
               {{"id",
                 Literals<GDF_INT32>{1,  2,  3,  4,  5,  6,  7,  8,  9,  10,
                                     11, 12, 13, 14, 15, 16, 17, 18, 19, 20}},
                {"value_", Literals<GDF_FLOAT64>{1,
                                                 115.175428317,
                                                 499.0596279328,
                                                 835.8426265411,
                                                 1292.473855301,
                                                 826.8534779598,
                                                 1934.5292318756,
                                                 1332.2415649832,
                                                 1611.0414216493,
                                                 1775.0670259674,
                                                 2116.596641313,
                                                 4138.9511402733,
                                                 1508.244383396,
                                                 1492.9359516858,
                                                 3443.8172654587,
                                                 3735.9505054879,
                                                 3084.5416272881,
                                                 3920.7155986259,
                                                 6058.7782725702,
                                                 7510.6685227154}}}}}
              .Build(),
      .resultTable =
          LiteralTableBuilder{
              "ResultSet",
              {{"GDF_FLOAT64", Literals<GDF_FLOAT64>{0,
                                                     4.7464564297,
                                                     6.2127255835,
                                                     6.7284403496,
                                                     7.1643133782,
                                                     6.7176275064,
                                                     7.5676192848,
                                                     7.1946181897,
                                                     7.3846360946,
                                                     7.4815934623,
                                                     7.6575647198,
                                                     8.328197687,
                                                     7.3187015934,
                                                     7.3084998975,
                                                     8.1443358053,
                                                     8.2257575513,
                                                     8.0341583442,
                                                     8.2740294668,
                                                     8.7092634536,
                                                     8.9240797585}}}}
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
      .query = "select ceil(value_), floor(value_) from main.emps",
      .logicalPlan =
          "LogicalProject(EXPR$0=[CEIL($1)], EXPR$1=[FLOOR($1)])\n  "
          "LogicalTableScan(table=[[main, emps]])",
      .tableGroup =
          LiteralTableGroupBuilder{
              {"main.emps",
               {{"id",
                 Literals<GDF_INT32>{1,  2,  3,  4,  5,  6,  7,  8,  9,  10,
                                     11, 12, 13, 14, 15, 16, 17, 18, 19, 20}},
                {"value_",
                 Literals<GDF_FLOAT64>{
                     281.2619471705,  56.5791917144,   335.7760856221,
                     399.3305728206,  83.1835183528,   192.9592203504,
                     196.5645757202,  -129.6411150196, -424.1815141328,
                     157.4281587579,  375.7787637601,  -493.8952370244,
                     -145.6943416199, 245.748108461,   -412.4347934558,
                     295.3340855468,  -255.7993803148, 445.3368308985,
                     246.5297262577,  -6.9296171121}}}}}
              .Build(),
      .resultTable =
          LiteralTableBuilder{
              "ResultSet",
              {{"GDF_FLOAT64",
                Literals<GDF_FLOAT64>{282,  57,   336,  400, 84,   193,  197,
                                      -129, -424, 158,  376, -493, -145, 246,
                                      -412, 296,  -255, 446, 247,  -6}},
               {"GDF_FLOAT64",
                Literals<GDF_FLOAT64>{281,  56,   335,  399, 83,   192,  196,
                                      -130, -425, 157,  375, -494, -146, 245,
                                      -413, 295,  -256, 445, 246,  -7}}}}
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
