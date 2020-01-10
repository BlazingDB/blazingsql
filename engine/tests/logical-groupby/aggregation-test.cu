//#include "gtest/gtest.h"

#include "src/operators/GroupBy.h"

#include <cuDF/utilities/column_wrapper.hpp>
#include <cuDF/utilities/base_fixture.hpp>
#include <cuDF/utilities/type_lists.hpp>
#include <cuDF/utilities/table_utilities.hpp>

template <typename T>
struct AggregationTest : public cudf::test::BaseFixture {};

TYPED_TEST_CASE(AggregationTest, cudf::test::NumericTypes);

TYPED_TEST(AggregationTest, CheckBasicWithGroupby) {

	//this this stuff
	//ral::operators::_new_aggregations_with_groupby()
	
}

TYPED_TEST(AggregationTest, GroupbyWithoutAggs) {

	using T = TypeParam;

	cudf::test::fixed_width_column_wrapper<T> col1{{5, 4, 3, 5, 8, 5, 6}, {1, 1, 1, 1, 1, 1, 1}};
	cudf::test::fixed_width_column_wrapper<T> col3{{10, 40, 70, 5, 2, 10, 11}, {1, 1, 1, 1, 1, 1, 1}};
	
	cudf::table_view cudf_table_in_view {{col1, col3}};

	std::vector<std::string> names({"A", "C"});
	ral::frame::BlazingTableView table(cudf_table_in_view, names);

	std::vector<int> group_column_indices{0};

	std::unique_ptr<ral::frame::BlazingTable> table_out = ral::operators::_new_groupby_without_aggregations(table, group_column_indices);

	if (std::is_same<T, cudf::experimental::bool8>::value) {
		cudf::test::fixed_width_column_wrapper<T> expect_col1{std::initializer_list<T> {1}, {true}};
		cudf::test::fixed_width_column_wrapper<T> expect_col3{std::initializer_list<T> {1}, {true}};

		CudfTableView expect_cudf_table_view {{expect_col1, expect_col3}};

		cudf::test::expect_tables_equal(expect_cudf_table_view, table_out->view());
	} else {
		cudf::test::fixed_width_column_wrapper<T> expect_col1{{3, 4, 5, 6 ,8}, {1, 1, 1, 1, 1}};
		cudf::test::fixed_width_column_wrapper<T> expect_col3{{70, 40, 10, 11, 2}, {1, 1, 1, 1, 1}};

		CudfTableView expect_cudf_table_view {{expect_col1, expect_col3}};

		cudf::test::expect_tables_equal(expect_cudf_table_view, table_out->view());
	}
}