#include <CalciteInterpreter.h>

#include <gdf/library/table_group.h>

#include "../BlazingUnitTest.h"
#include <gtest/gtest.h>

class Item {
public:
	const std::string name;
	const std::string plan;
	const std::vector<gdf::library::DType<GDF_FLOAT64>::value_type> result;
};

class WhereFrom2e2Test : public testing::TestWithParam<Item> {
	void SetUp() { rmmInitialize(nullptr); }
};

using gdf::library::Literals;
using gdf::library::LiteralTableBuilder;
using gdf::library::TableGroup;

TEST_P(WhereFrom2e2Test, CompareLiteral) {
	auto table = LiteralTableBuilder{"main.holas",
		{
			{
				"swings",
				Literals<GDF_FLOAT64>{1, 2, 3, 4, 5, 6, 7},
			},
			{
				"tractions",
				Literals<GDF_FLOAT64>{1, 0, 1, 0, -1, 0, -1},
			},
		}}.Build();
	auto group = TableGroup{table};

	const std::string & plan = GetParam().plan;

	std::vector<gdf_column_cpp> output;

	gdf_error status = interpreter_wrapper::evaluate_query(
		group.ToBlazingFrame(), group.table_names(), group.column_names(), plan, output);
	EXPECT_EQ(GDF_SUCCESS, status);

	auto result = gdf::library::GdfColumnCppsTableBuilder{"ResultTable", output}.Build();

	auto expected = LiteralTableBuilder{"ResultTable",
		{
			{"", Literals<GDF_FLOAT64>{std::move(GetParam().result)}},
		}}.Build();

	EXPECT_EQ(expected, result);
}

class ItemParamName {
public:
	template <class ParamType>
	std::string operator()(const testing::TestParamInfo<ParamType> & info) const {
		return info.param.name;
	}
};

INSTANTIATE_TEST_CASE_P(SwingsCompareWithOne,
	WhereFrom2e2Test,
	testing::ValuesIn({
		Item{
			"EqualsOne",
			"LogicalProject(swings=[$0])\n"
			"  LogicalFilter(condition=[=($1, 1)])\n"
			"    LogicalTableScan(table=[[main, holas]])",
			{1, 3},
		},
		Item{
			"LessThanOne",
			"LogicalProject(swings=[$0])\n"
			"  LogicalFilter(condition=[<($1, 1)])\n"
			"    LogicalTableScan(table=[[main, holas]])",
			{2, 4, 5, 6, 7},
		},
		Item{
			"GreaterThanOne",
			"LogicalProject(swings=[$0])\n"
			"  LogicalFilter(condition=[>($1, 1)])\n"
			"    LogicalTableScan(table=[[main, holas]])",
			{},
		},
	}),
	ItemParamName());

INSTANTIATE_TEST_CASE_P(SwingsCompareWithNegativeOne,
	WhereFrom2e2Test,
	testing::ValuesIn({
		Item{
			"EqualsNegativeOne",
			"LogicalProject(swings=[$0])\n"
			"  LogicalFilter(condition=[=($1, -1)])\n"
			"    LogicalTableScan(table=[[main, holas]])",
			{5, 7},
		},
		Item{
			"LessThanNegativeOne",
			"LogicalProject(swings=[$0])\n"
			"  LogicalFilter(condition=[<($1, -1)])\n"
			"    LogicalTableScan(table=[[main, holas]])",
			{},
		},
		Item{
			"GreaterThanNegativeOne",
			"LogicalProject(swings=[$0])\n"
			"  LogicalFilter(condition=[>($1, -1)])\n"
			"    LogicalTableScan(table=[[main, holas]])",
			{1, 2, 3, 4, 6},
		},
	}),
	ItemParamName());
