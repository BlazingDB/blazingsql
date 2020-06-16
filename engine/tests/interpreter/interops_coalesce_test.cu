
#include <cstdlib>
#include <iostream>
#include <string>
#include <vector>

#include "Interpreter/interpreter_cpp.h"
#include "Interpreter/interpreter_ops.cuh"

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

struct InputTestItem
{
    std::string query;
    std::string logicalPlan;
    gdf::library::TableGroup tableGroup;
};

struct EvaluateQueryTest : public ::testing::Test
{

    InputTestItem input;
    EvaluateQueryTest() : input{InputTestItem{
                              .query = "select id + -1 from main.emps",
                              .logicalPlan =
                                  "LogicalProject(EXPR$0=[CASE(IS NOT NULL($0), $0, -1)])\n  "
                                  "LogicalTableScan(table=[[main, emps]])",
                              .tableGroup =
                                  LiteralTableGroupBuilder{
                                      {"main.emps",
                                       {{"id", Literals<GDF_INT32>{Literals<GDF_INT32>::vector{1, 2, 3, 4, 5, 6, 7, 8, 9, 1}, Literals<GDF_INT32>::bool_vector{0, 1, 1, 1, 0, 0, 0, 0, 1, 1}}},
                                        {"age",  Literals<GDF_INT32>{Literals<GDF_INT32>::vector{10, 20, 10, 20, 10, 20, 10, 20, 10, 2}, Literals<GDF_INT32>::bool_vector{1, 1, 1, 1, 0, 0, 0, 0, 1, 1}}},
                                        {"salary", Literals<GDF_INT32>{Literals<GDF_INT32>::vector{9000, 8000, 7000, 6000, 5000, 4000, 3000, 2000, 1000, 0}, Literals<GDF_INT32>::bool_vector{1, 1, 1, 1, 1, 1, 1, 1, 1, 1}}}
                                       }
                                       }}.Build()}}
    {
    }
   void SetUp() {
    
  }

    void CHECK_RESULT(gdf::library::Table &computed_solution,
                      gdf::library::Table &reference_solution)
    {
        computed_solution.print(std::cout);
        reference_solution.print(std::cout);

        for (size_t index = 0; index < reference_solution.size(); index++)
        {
            const auto &reference_column = reference_solution[index];
            const auto &computed_column = computed_solution[index];

            auto reference_valids = reference_column.getValids();
            auto solution_valids = computed_column.getValids();
            EXPECT_TRUE(reference_valids == solution_valids);

            auto a = reference_column.to_string();
            auto b = computed_column.to_string();
            EXPECT_EQ(a, b);
        }
    }
};

// LogicalTableScan(table=[[main, emps]])
TEST_F(EvaluateQueryTest, TEST_01)
{
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

     output_table.print(std::cout);
    // CHECK_RESULT(output_table, input.resultTable);
} 
