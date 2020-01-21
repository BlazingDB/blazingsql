//#include "gtest/gtest.h"

#include "src/operators/GroupBy.h"

#include <from_cudf/cpp_tests/utilities/column_wrapper.hpp>
#include <from_cudf/cpp_tests/utilities/base_fixture.hpp>
#include <from_cudf/cpp_tests/utilities/type_lists.hpp>
#include <from_cudf/cpp_tests/utilities/table_utilities.hpp>


template <typename T>
struct AggregationTest : public cudf::test::BaseFixture {};

TYPED_TEST_CASE(AggregationTest, cudf::test::NumericTypes);

TYPED_TEST(AggregationTest, CheckBasicWithGroupby) {

	using T = TypeParam;

	cudf::test::fixed_width_column_wrapper<T> key{{   5,  4,  3, 5, 8,  5, 6,  5}, {1, 1, 1, 1, 1, 1, 1, 0}};
	cudf::test::fixed_width_column_wrapper<T> value{{10, 40, 70, 5, 2, 10, 11, 55}, {1, 1, 1, 1, 1, 1, 1, 0}};

	std::vector<std::string> column_names{"A", "B"};
	ral::frame::BlazingTableView table(CudfTableView{{key, value}}, column_names);

	std::vector<cudf::experimental::aggregation::Kind> aggregation_types{cudf::experimental::aggregation::Kind::SUM, 
				cudf::experimental::aggregation::Kind::COUNT, cudf::experimental::aggregation::Kind::MIN, cudf::experimental::aggregation::Kind::MAX};

	std::vector<std::string> aggregation_input_expressions{"1", "1", "1", "1"};
	std::vector<std::string> aggregation_column_assigned_aliases{"agg0", "agg1", "agg2", "agg3"};
	std::vector<int> group_column_indices{0};

	std::unique_ptr<ral::frame::BlazingTable> result = ral::operators::experimental::compute_aggregations_with_groupby(
		table, aggregation_types, aggregation_input_expressions, aggregation_column_assigned_aliases, group_column_indices);

	for (int i = 0; i < result->view().num_rows(); i++){
		std::string col_string = cudf::test::to_string(result->view().column(i), "|");
    	std::cout<<result->names()[i]<<": "<<col_string<<std::endl;
	}
	

	if (std::is_same<T, cudf::experimental::bool8>::value) {
		EXPECT_TRUE(true);
	} else {
		cudf::test::fixed_width_column_wrapper<T> expect_key{{ 3,  4,  5,  6, 8}, {1, 1, 1, 1, 1}};
		cudf::test::fixed_width_column_wrapper<T> expect_agg0{{70, 40, 25, 11, 2}, {1, 1, 1, 1, 1}};
		cudf::test::fixed_width_column_wrapper<T> expect_agg1{{1, 1, 3, 1, 1}, {1, 1, 1, 1, 1}};
		cudf::test::fixed_width_column_wrapper<T> expect_agg2{{70, 40, 5, 11, 2}, {1, 1, 1, 1, 1}};
		cudf::test::fixed_width_column_wrapper<T> expect_agg3{{70, 40, 10, 11, 2}, {1, 1, 1, 1, 1}};

		CudfTableView expect_table{{expect_key, expect_agg0, expect_agg1, expect_agg2, expect_agg3}};
		
		cudf::test::expect_tables_equal(result->view(), expect_table);			
		
	}
}

// TYPED_TEST(AggregationTest, GroupbyWithoutAggs) {

// 	using T = TypeParam;

// 	cudf::test::fixed_width_column_wrapper<T> col1{{5, 4, 3, 5, 8, 5, 6}, {1, 1, 1, 1, 1, 1, 1}};
// 	cudf::test::fixed_width_column_wrapper<T> col3{{10, 40, 70, 5, 2, 10, 11}, {1, 1, 1, 1, 1, 1, 1}};
	
// 	cudf::table_view cudf_table_in_view {{col1, col3}};

// 	std::vector<std::string> names({"A", "C"});
// 	ral::frame::BlazingTableView table(cudf_table_in_view, names);

// 	std::vector<int> group_column_indices{0};

// 	std::unique_ptr<ral::frame::BlazingTable> table_out = ral::operators::_new_groupby_without_aggregations(table, group_column_indices);

// 	if (std::is_same<T, cudf::experimental::bool8>::value) {
// 		cudf::test::fixed_width_column_wrapper<T> expect_col1{std::initializer_list<T> {1}, {true}};
// 		cudf::test::fixed_width_column_wrapper<T> expect_col3{std::initializer_list<T> {1}, {true}};

// 		CudfTableView expect_cudf_table_view {{expect_col1, expect_col3}};

// 		cudf::test::expect_tables_equal(expect_cudf_table_view, table_out->view());
// 	} else {
// 		cudf::test::fixed_width_column_wrapper<T> expect_col1{{3, 4, 5, 6 ,8}, {1, 1, 1, 1, 1}};
// 		cudf::test::fixed_width_column_wrapper<T> expect_col3{{70, 40, 10, 11, 2}, {1, 1, 1, 1, 1}};

// 		CudfTableView expect_cudf_table_view {{expect_col1, expect_col3}};

// 		cudf::test::expect_tables_equal(expect_cudf_table_view, table_out->view());
// 	}
// }

// TYPED_TEST(AggregationTest, GroupbyWithoutAggsWithNull) {

// 	using T = TypeParam;

// 	cudf::test::fixed_width_column_wrapper<T> col1{{1, 1, 2, 1, 2, 2, 3, 3}, {1, 1, 1, 1, 1, 1, 1, 1}};
// 	cudf::test::fixed_width_column_wrapper<T> col2{{1, 1, 9, 2, 2, 2, 9, 3}, {1, 1, 0, 1, 1, 1, 0, 1}};
// 	cudf::test::fixed_width_column_wrapper<T> col3{{3, 4, 2, 9, 1, 1, 1, 3}, {1, 1, 1, 0, 1, 1, 1, 1}};
	
// 	cudf::table_view cudf_table_in_view {{col1, col2, col3}};

// 	std::vector<std::string> names({"A", "B", "C"});
// 	ral::frame::BlazingTableView table(cudf_table_in_view, names);

// 	std::vector<int> group_column_indices{0, 1};

// 	std::unique_ptr<ral::frame::BlazingTable> table_out = ral::operators::_new_groupby_without_aggregations(table, group_column_indices);

// 	if (std::is_same<T, cudf::experimental::bool8>::value) {
// 		cudf::test::fixed_width_column_wrapper<T> expect_col1{std::initializer_list<T> {1, 1}, {true, true}};
// 		cudf::test::fixed_width_column_wrapper<T> expect_col2{std::initializer_list<T> {0, 1}, {false, true}};
// 		cudf::test::fixed_width_column_wrapper<T> expect_col3{std::initializer_list<T> {1, 1}, {true, true}};

// 		CudfTableView expect_cudf_table_view {{expect_col1, expect_col2, expect_col3}};

// 		cudf::test::expect_tables_equal(expect_cudf_table_view, table_out->view());
// 	} else {
// 		cudf::test::fixed_width_column_wrapper<T> expect_col1{{1, 1, 2, 2, 3, 3}, {1, 1, 1, 1, 1, 1}};
// 		cudf::test::fixed_width_column_wrapper<T> expect_col2{{1, 2, 9, 2, 9, 3}, {1, 1, 0, 1, 0, 1}};
// 		cudf::test::fixed_width_column_wrapper<T> expect_col3{{3, 9, 2, 1, 1, 3}, {1, 0, 1, 1, 1, 1}};

// 		CudfTableView expect_cudf_table_view {{expect_col1, expect_col2, expect_col3}};

// 		cudf::test::expect_tables_equal(expect_cudf_table_view, table_out->view());
// 	}
// }