//#include "gtest/gtest.h"

#include <cudf/sorting.hpp>

#include <operators/GroupBy.h>
#include <tests/utilities/column_wrapper.hpp>
#include <tests/utilities/base_fixture.hpp>
#include <tests/utilities/type_lists.hpp>
#include <tests/utilities/table_utilities.hpp>
#include <tests/utilities/column_utilities.hpp>
#include "tests/utilities/BlazingUnitTest.h"

 template <typename T>
struct AggregationTest : public BlazingUnitTest {};


using DecimalTypes = cudf::test::Types<int8_t, int16_t, int32_t, int64_t>;

TYPED_TEST_CASE(AggregationTest, DecimalTypes);

TYPED_TEST(AggregationTest, CheckBasicWithGroupby) {

 	using T = TypeParam;


	cudf::test::fixed_width_column_wrapper<T> key{{   5,  4,  3, 5, 8,  5, 6, 5}, {1, 1, 1, 1, 1, 1, 1, 0}};
	cudf::test::fixed_width_column_wrapper<T> value{{10, 40, 70, 5, 2, 10, 11, 55}, {1, 1, 1, 1, 1, 1, 1, 0}};

	std::vector<std::string> column_names{"A", "B"};
	ral::frame::BlazingTableView table(CudfTableView{{key, value}}, column_names);

	std::vector<AggregateKind> aggregation_types{AggregateKind::SUM, AggregateKind::COUNT_VALID, 
				AggregateKind::MIN, AggregateKind::MAX};

	std::vector<std::string> aggregation_input_expressions{"1", "1", "1", "1"};
	std::vector<std::string> aggregation_column_assigned_aliases{"agg0", "agg1", "agg2", "agg3"};
	std::vector<int> group_column_indices{0};

	std::unique_ptr<ral::frame::BlazingTable> result = ral::operators::compute_aggregations_with_groupby(
		table, aggregation_input_expressions, aggregation_types, aggregation_column_assigned_aliases, group_column_indices);

	// for (int i = 0; i < result->view().num_columns(); i++){
	// 	std::string col_string = cudf::test::to_string(result->view().column(i), "|");
    // 	std::cout<<result->names()[i]<<": "<<col_string<<std::endl;
	// }
	

	cudf::test::fixed_width_column_wrapper<T> expect_key{{ 3,  4,  5,  6, 8, 0}, {1, 1, 1, 1, 1, 0}};
	cudf::test::fixed_width_column_wrapper<int64_t> expect_agg0{{70, 40, 25, 11, 2, 0}, {1, 1, 1, 1, 1, 0}};
	cudf::test::fixed_width_column_wrapper<int32_t> expect_agg1{{1, 1, 3, 1, 1, 0}, {1, 1, 1, 1, 1, 1}};
	cudf::test::fixed_width_column_wrapper<T> expect_agg2{{70, 40, 5, 11, 2, 0}, {1, 1, 1, 1, 1, 0}};
	cudf::test::fixed_width_column_wrapper<T> expect_agg3{{70, 40, 10, 11, 2, 0}, {1, 1, 1, 1, 1, 0}};

	CudfTableView expect_table{{expect_key, expect_agg0, expect_agg1, expect_agg2, expect_agg3}};

	std::unique_ptr<cudf::table> sorted_result = cudf::sort_by_key(result->view(),result->view().select({0}));
	std::unique_ptr<cudf::table> sorted_expected = cudf::sort_by_key(expect_table,expect_table.select({0}));
  	
	cudf::test::expect_tables_equivalent(sorted_result->view(), sorted_expected->view());						
}

TYPED_TEST(AggregationTest, MoreComplexGroupby) {

 	using T = TypeParam;


	cudf::test::fixed_width_column_wrapper<T> key{{   5,  4,  3, 5, 8,  5, 6, 5}, {1, 1, 1, 1, 1, 1, 1, 0}};
	cudf::test::fixed_width_column_wrapper<T> value{{10, 40, 70, 5, 2, 10, 11, 55}, {1, 1, 1, 1, 1, 1, 1, 0}};

	std::vector<std::string> column_names{"A", "B"};
	ral::frame::BlazingTableView table(CudfTableView{{key, value}}, column_names);

	std::vector<AggregateKind> aggregation_types{AggregateKind::SUM, AggregateKind::COUNT_VALID, 
				AggregateKind::SUM, AggregateKind::COUNT_VALID};

	std::vector<std::string> aggregation_input_expressions{"$1", "$1", "$0", "$0"};
	std::vector<std::string> aggregation_column_assigned_aliases{"", "", "", ""};
	std::vector<int> group_column_indices{0};

	std::unique_ptr<ral::frame::BlazingTable> result = ral::operators::compute_aggregations_with_groupby(
		table, aggregation_input_expressions, aggregation_types, aggregation_column_assigned_aliases, group_column_indices);

	// for (int i = 0; i < result->view().num_columns(); i++){
	// 	std::string col_string = cudf::test::to_string(result->view().column(i), "|");
    // 	std::cout<<result->names()[i]<<" : "<<col_string<<std::endl;
	// }
	

	cudf::test::fixed_width_column_wrapper<T> expect_key{{ 3,  4,  5,  6, 8, 0}, {1, 1, 1, 1, 1, 0}};
	cudf::test::fixed_width_column_wrapper<int64_t> expect_agg0{{70, 40, 25, 11, 2, 0}, {1, 1, 1, 1, 1, 0}};
	cudf::test::fixed_width_column_wrapper<int32_t> expect_agg1{{1, 1, 3, 1, 1, 0}, {1, 1, 1, 1, 1, 1}};
	cudf::test::fixed_width_column_wrapper<int64_t> expect_agg2{{ 3,  4,  15,  6, 8, 0}, {1, 1, 1, 1, 1, 0}};
	cudf::test::fixed_width_column_wrapper<int32_t> expect_agg3{{1, 1, 3, 1, 1, 0}, {1, 1, 1, 1, 1, 1}};

	CudfTableView expect_table{{expect_key, expect_agg0, expect_agg1, expect_agg2, expect_agg3}};
	
	std::unique_ptr<cudf::table> sorted_result = cudf::sort_by_key(result->view(),result->view().select({0}));
	std::unique_ptr<cudf::table> sorted_expected = cudf::sort_by_key(expect_table,expect_table.select({0}));
  	
	cudf::test::expect_tables_equivalent(sorted_result->view(), sorted_expected->view());						
}


TYPED_TEST(AggregationTest, GroupbyWithoutAggs) {

using T = TypeParam;


	cudf::test::fixed_width_column_wrapper<T> col1{{ 5,  4,  3, 5, 8,  5,  6}, {1, 1, 1, 1, 1, 1, 1}};
	cudf::test::fixed_width_column_wrapper<T> col2{{10, 40, 70, 5, 2, 10, 11}, {1, 1, 1, 1, 1, 1, 1}};

	std::vector<std::string> column_names{"A", "B"};
	ral::frame::BlazingTableView table(CudfTableView{{col1, col2}}, column_names);

	std::vector<int> group_column_indices{0, 1};

	std::unique_ptr<ral::frame::BlazingTable> result = ral::operators::compute_groupby_without_aggregations(
		table, group_column_indices);

	// for (int i = 0; i < result->view().num_columns(); i++){
	// 	std::string col_string = cudf::test::to_string(result->view().column(i), "|");
    // 	std::cout<<result->names()[i]<<" : "<<col_string<<std::endl;
	// }
	
	cudf::test::fixed_width_column_wrapper<T> expect_col1{{3,  4,  5,  5,  6, 8}, {1, 1, 1, 1, 1, 1}};
	cudf::test::fixed_width_column_wrapper<T> expect_col2{{70, 40, 5, 10, 11, 2}, {1, 1, 1, 1, 1, 1}};

	CudfTableView expect_table{{expect_col1, expect_col2}};
	
	std::unique_ptr<cudf::table> sorted_result = cudf::sort_by_key(result->view(),result->view().select({0}));
	std::unique_ptr<cudf::table> sorted_expected = cudf::sort_by_key(expect_table,expect_table.select({0}));
  	
	cudf::test::expect_tables_equivalent(sorted_result->view(), sorted_expected->view());										
}


 TYPED_TEST(AggregationTest, AggsWithoutGroupbyWithNull) {

 	using T = TypeParam;

 	cudf::test::fixed_width_column_wrapper<T> col1{{1, 1, 2, 1, 2, 2, 3, 3}, {1, 1, 1, 1, 1, 1, 1, 1}};
 	cudf::test::fixed_width_column_wrapper<T> col2{{1, 1, 9, 2, 2, 2, 9, 3}, {1, 1, 0, 1, 1, 1, 0, 1}};
 	cudf::test::fixed_width_column_wrapper<T> col3{{3, 4, 2, 9, 1, 1, 1, 3}, {1, 1, 1, 0, 1, 1, 1, 1}};
	
	std::vector<std::string> column_names{"A", "B", "C"};
	ral::frame::BlazingTableView table(CudfTableView{{col1, col2, col3}}, column_names);

	std::vector<AggregateKind> aggregation_types{AggregateKind::SUM, AggregateKind::SUM, AggregateKind::COUNT_VALID,
				AggregateKind::SUM, AggregateKind::COUNT_VALID, AggregateKind::COUNT_ALL, AggregateKind::MAX, AggregateKind::MAX};
				
	std::vector<std::string> aggregation_input_expressions{"0", "1", "2", "2", "1", "", "1", "2"};
	std::vector<std::string> aggregation_column_assigned_aliases{"agg0", "agg1", "agg2", "agg3", "cstar","agg4", "agg5", "agg6"};
	
	std::unique_ptr<ral::frame::BlazingTable> result = ral::operators::compute_aggregations_without_groupby(
		table, aggregation_input_expressions, aggregation_types, aggregation_column_assigned_aliases);

	// for (int i = 0; i < result->view().num_columns(); i++){
	// 	std::string col_string = cudf::test::to_string(result->view().column(i), "|");
    // 	std::cout<<result->names()[i]<<" : "<<col_string<<std::endl;
	// }
	
	cudf::test::fixed_width_column_wrapper<int64_t> expect_agg0{{15}};
	cudf::test::fixed_width_column_wrapper<int64_t> expect_agg1{{11}};
	cudf::test::fixed_width_column_wrapper<int64_t> expect_agg2{{7}};
	cudf::test::fixed_width_column_wrapper<int64_t> expect_agg3{{15}};
	cudf::test::fixed_width_column_wrapper<int64_t> expect_agg4{{6}};
	cudf::test::fixed_width_column_wrapper<int64_t> expect_agg5{{8}};
	cudf::test::fixed_width_column_wrapper<T> expect_agg6{{3}};
	cudf::test::fixed_width_column_wrapper<T> expect_agg7{{4}};
	

	CudfTableView expect_table{{expect_agg0, expect_agg1, expect_agg2, expect_agg3, expect_agg4, expect_agg5, expect_agg6, expect_agg7}};

	cudf::test::expect_tables_equivalent(result->view(), expect_table);										

}
