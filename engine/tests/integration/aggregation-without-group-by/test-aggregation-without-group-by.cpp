#include <cstdlib>
#include <iostream>
#include <string>
#include <vector>

#include <CalciteExpressionParsing.h>
#include <CalciteInterpreter.h>
#include <DataFrame.h>
#include <GDFColumn.cuh>
#include <GDFCounter.cuh>
#include <Utils.cuh>
#include <blazingdb/io/Util/StringUtil.h>
#include <gtest/gtest.h>

#include "../../query_test.h"
#include "gdf/library/api.h"

using namespace gdf::library;

struct EvaluateQueryTest : public query_test {
	struct InputTestItem {
		std::string query;
		std::string logicalPlan;
		gdf::library::TableGroup tableGroup;
		gdf::library::Table resultTable;
	};

	void CHECK_RESULT(gdf::library::Table & computed_solution, gdf::library::Table & reference_solution) {
		computed_solution.print(std::cout);
		reference_solution.print(std::cout);

		for(size_t index = 0; index < reference_solution.size(); index++) {
			const auto & reference_column = reference_solution[index];
			const auto & computed_column = computed_solution[index];
			auto a = reference_column.to_string();
			auto b = computed_column.to_string();
			EXPECT_EQ(a, b);
		}
	}

	void CHECK_RESULT(gdf::library::Table & computed_solution,
		gdf::library::Table & reference_solution,
		std::vector<int> & valid_result) {
		computed_solution.print(std::cout);
		reference_solution.print(std::cout);

		for(size_t index = 0; index < reference_solution.size(); index++) {
			const auto & reference_column = reference_solution[index];
			const auto & computed_column = computed_solution[index];

			int valid = 1;
			if(computed_column.getValids().size() > 0)
				valid = ((int) computed_column.getValids()[0] & 1);

			EXPECT_TRUE(valid == valid_result[index]);

			if(valid_result[index]) {
				auto com_val = (int64_t) computed_column[index];
				auto ref_val = (int64_t) reference_column[index];
				EXPECT_TRUE(ref_val == com_val);
			}
		}
	}
};


TEST_F(EvaluateQueryTest, TEST_01) {
	auto input = InputTestItem{.query =
								   "select count(p_partkey), sum(p_partkey), avg(p_partkey), "
								   "max(p_partkey), min(p_partkey) from main.part",
		.logicalPlan =
			"LogicalAggregate(group=[{}], EXPR$0=[COUNT()], EXPR$1=[SUM($0)], "
			"EXPR$2=[AVG($0)], EXPR$3=[MAX($0)], EXPR$4=[MIN($0)])\n  "
			"LogicalTableScan(table=[[main, part]])",
		.tableGroup = LiteralTableGroupBuilder{{"main.part",
												   {{"p_partkey",
													   Literals<GDF_INT32>{1,
														   2,
														   3,
														   4,
														   5,
														   6,
														   7,
														   8,
														   9,
														   10,
														   11,
														   12,
														   13,
														   14,
														   15,
														   16,
														   17,
														   18,
														   19,
														   20,
														   21,
														   22,
														   23,
														   24,
														   25,
														   26,
														   27,
														   28,
														   29,
														   30,
														   31,
														   32,
														   33,
														   34,
														   35,
														   36,
														   37,
														   38,
														   39,
														   40,
														   41,
														   42,
														   43,
														   44,
														   45,
														   46,
														   47,
														   48,
														   49,
														   50,
														   51,
														   52,
														   53,
														   54,
														   55,
														   56,
														   57,
														   58,
														   59,
														   60,
														   61,
														   62,
														   63,
														   64,
														   65,
														   66,
														   67,
														   68,
														   69,
														   70,
														   71,
														   72,
														   73,
														   74,
														   75,
														   76,
														   77,
														   78,
														   79,
														   80,
														   81,
														   82,
														   83,
														   84,
														   85,
														   86,
														   87,
														   88,
														   89,
														   90,
														   91,
														   92,
														   93,
														   94,
														   95,
														   96,
														   97,
														   98,
														   99,
														   100,
														   101,
														   102,
														   103,
														   104,
														   105,
														   106,
														   107,
														   108,
														   109,
														   110,
														   111,
														   112,
														   113,
														   114,
														   115,
														   116,
														   117,
														   118,
														   119,
														   120,
														   121,
														   122,
														   123,
														   124,
														   125,
														   126,
														   127,
														   128,
														   129,
														   130,
														   131,
														   132,
														   133,
														   134,
														   135,
														   136,
														   137,
														   138,
														   139,
														   140,
														   141,
														   142,
														   143,
														   144,
														   145,
														   146,
														   147,
														   148,
														   149,
														   150,
														   151,
														   152,
														   153,
														   154,
														   155,
														   156,
														   157,
														   158,
														   159,
														   160,
														   161,
														   162,
														   163,
														   164,
														   165,
														   166,
														   167,
														   168,
														   169,
														   170,
														   171,
														   172,
														   173,
														   174,
														   175,
														   176,
														   177,
														   178,
														   179,
														   180,
														   181,
														   182,
														   183,
														   184,
														   185,
														   186,
														   187,
														   188,
														   189,
														   190,
														   191,
														   192,
														   193,
														   194,
														   195,
														   196,
														   197,
														   198,
														   199,
														   200}}}}}
						  .Build(),
		.resultTable = LiteralTableBuilder{"ResultSet",
			{{"GDF_INT64", Literals<GDF_INT64>{200}},
				{"GDF_INT64", Literals<GDF_INT64>{20100}},
				{"GDF_FLOAT64", Literals<GDF_FLOAT64>{100.5}},
				{"GDF_INT64", Literals<GDF_INT64>{200}},
				{"GDF_INT64", Literals<GDF_INT64>{1}}}}
						   .Build()};
	auto logical_plan = input.logicalPlan;
	auto input_tables = input.tableGroup.ToBlazingFrame();
	auto table_names = input.tableGroup.table_names();
	auto column_names = input.tableGroup.column_names();
	std::vector<gdf_column_cpp> outputs;
	gdf_error err = evaluate_query(input_tables, table_names, column_names, logical_plan, outputs);
	EXPECT_TRUE(err == GDF_SUCCESS);
	auto output_table = GdfColumnCppsTableBuilder{"output_table", outputs}.Build();
	CHECK_RESULT(output_table, input.resultTable);
}


TEST_F(EvaluateQueryTest, TEST_03) {
	auto input = InputTestItem{.query =
								   "select count(p_partkey), sum(p_partkey), avg(p_partkey), "
								   "max(p_partkey), min(p_partkey) from main.part where p_partkey < 0",
		.logicalPlan =
			"LogicalAggregate(group=[{}], EXPR$0=[COUNT()], EXPR$1=[SUM($0)], EXPR$2=[AVG($0)], EXPR$3=[MAX($0)], "
			"EXPR$4=[MIN($0)])\n"
			"  LogicalFilter(condition=[<($0, 0)])\n"
			"    LogicalTableScan(table=[[main, part]])",
		.tableGroup = LiteralTableGroupBuilder{{"main.part",
												   {{"p_partkey",
													   Literals<GDF_INT32>{1,
														   2,
														   3,
														   4,
														   5,
														   6,
														   7,
														   8,
														   9,
														   10,
														   11,
														   12,
														   13,
														   14,
														   15,
														   16,
														   17,
														   18,
														   19,
														   20,
														   21,
														   22,
														   23,
														   24,
														   25,
														   26,
														   27,
														   28,
														   29,
														   30,
														   31,
														   32,
														   33,
														   34,
														   35,
														   36,
														   37,
														   38,
														   39,
														   40,
														   41,
														   42,
														   43,
														   44,
														   45,
														   46,
														   47,
														   48,
														   49,
														   50,
														   51,
														   52,
														   53,
														   54,
														   55,
														   56,
														   57,
														   58,
														   59,
														   60,
														   61,
														   62,
														   63,
														   64,
														   65,
														   66,
														   67,
														   68,
														   69,
														   70,
														   71,
														   72,
														   73,
														   74,
														   75,
														   76,
														   77,
														   78,
														   79,
														   80,
														   81,
														   82,
														   83,
														   84,
														   85,
														   86,
														   87,
														   88,
														   89,
														   90,
														   91,
														   92,
														   93,
														   94,
														   95,
														   96,
														   97,
														   98,
														   99,
														   100,
														   101,
														   102,
														   103,
														   104,
														   105,
														   106,
														   107,
														   108,
														   109,
														   110,
														   111,
														   112,
														   113,
														   114,
														   115,
														   116,
														   117,
														   118,
														   119,
														   120,
														   121,
														   122,
														   123,
														   124,
														   125,
														   126,
														   127,
														   128,
														   129,
														   130,
														   131,
														   132,
														   133,
														   134,
														   135,
														   136,
														   137,
														   138,
														   139,
														   140,
														   141,
														   142,
														   143,
														   144,
														   145,
														   146,
														   147,
														   148,
														   149,
														   150,
														   151,
														   152,
														   153,
														   154,
														   155,
														   156,
														   157,
														   158,
														   159,
														   160,
														   161,
														   162,
														   163,
														   164,
														   165,
														   166,
														   167,
														   168,
														   169,
														   170,
														   171,
														   172,
														   173,
														   174,
														   175,
														   176,
														   177,
														   178,
														   179,
														   180,
														   181,
														   182,
														   183,
														   184,
														   185,
														   186,
														   187,
														   188,
														   189,
														   190,
														   191,
														   192,
														   193,
														   194,
														   195,
														   196,
														   197,
														   198,
														   199,
														   200}}}}}
						  .Build(),
		.resultTable = LiteralTableBuilder{"ResultSet",
			{{"GDF_INT64", Literals<GDF_INT64>{0}},
				{"GDF_INT64", Literals<GDF_INT64>{0}},
				{"GDF_INT64", Literals<GDF_INT64>{0}},
				{"GDF_INT64", Literals<GDF_INT64>{0}},
				{"GDF_INT64", Literals<GDF_INT64>{0}}}}
						   .Build()};

	auto logical_plan = input.logicalPlan;
	auto input_tables = input.tableGroup.ToBlazingFrame();
	auto table_names = input.tableGroup.table_names();
	auto column_names = input.tableGroup.column_names();
	std::vector<gdf_column_cpp> outputs;
	gdf_error err = evaluate_query(input_tables, table_names, column_names, logical_plan, outputs);

	EXPECT_TRUE(err == GDF_SUCCESS);

	auto output_table = GdfColumnCppsTableBuilder{"output_table", outputs}.Build();
	std::cout << "output with nulls\n";
	output_table.print(std::cout);
	std::vector<int> valid_result{1, 0, 0, 0, 0};
	CHECK_RESULT(output_table, input.resultTable, valid_result);
}
