
#include <cstdlib>
#include <iostream>
#include <string>
#include <vector>

#include "operators/OrderBy.cpp"

#include <execution_graph/logic_controllers/LogicPrimitives.h>

#include <from_cudf/cpp_tests/utilities/column_wrapper.hpp>
#include <from_cudf/cpp_tests/utilities/base_fixture.hpp>
#include <from_cudf/cpp_tests/utilities/type_lists.hpp>
#include <from_cudf/cpp_tests/utilities/table_utilities.hpp>
#include <cudf/detail/gather.hpp>

#include <operators/OrderBy.h>

template <typename T>
struct SortTest : public cudf::test::BaseFixture {};

TYPED_TEST_CASE(SortTest, cudf::test::NumericTypes);

TYPED_TEST(SortTest, withoutNull) {

    using T = TypeParam;

    blazing_frame input;
    std::vector<cudf::column_view> rawCols;
    std::vector<gdf_column_cpp> sortedTable;

    cudf::test::fixed_width_column_wrapper<T> col1{{4, 5, 3, 5, 8, 5, 6}, {1, 1, 1, 1, 1, 1, 1}};
    cudf::test::strings_column_wrapper col2({"b", "d", "a", "d", "l", "d", "k"}, {1, 1, 1, 1, 1, 1, 1});
    cudf::test::fixed_width_column_wrapper<T> col3{{10, 40, 70, 5, 2, 10, 11}, {1, 1, 1, 1, 1, 1, 1}};

    CudfTableView cudf_table_in_view {{col1, col2, col3}};

    std::vector<std::string> names({"A", "B", "C"});
    ral::frame::BlazingTableView table(cudf_table_in_view, names);

    std::vector<int> sortColIndices{0,1};
    std::vector<int8_t> sortOrderTypes{0, 0};

    std::unique_ptr<ral::frame::BlazingTable> table_out = ral::operators::experimental::logicalSort(table, sortColIndices, sortOrderTypes);

    cudf::test::fixed_width_column_wrapper<T> expect_col1{{3, 4, 5, 5, 5, 6 ,8}, {1, 1, 1, 1, 1, 1, 1}};
    cudf::test::strings_column_wrapper expect_col2({"a", "b", "d", "d", "d", "k", "l"}, {1, 1, 1, 1, 1, 1, 1});
    cudf::test::fixed_width_column_wrapper<T> expect_col3{{70, 10, 40, 5, 10, 11, 2}, {1, 1, 1, 1, 1, 1, 1}};
    CudfTableView expect_cudf_table_view {{expect_col1, expect_col2, expect_col3}};

    cudf::test::expect_tables_equal(expect_cudf_table_view, table_out->view());
}

template <typename T>
struct LimitTest : public cudf::test::BaseFixture {};

TYPED_TEST_CASE(LimitTest, cudf::test::NumericTypes);

TYPED_TEST(LimitTest, withoutNull) {

    using T = TypeParam;

    blazing_frame input;
    std::vector<cudf::column_view> rawCols;
    std::vector<gdf_column_cpp> sortedTable;

    cudf::test::fixed_width_column_wrapper<T> col1{{5, 4, 3, 5, 8, 5, 6}, {1, 1, 1, 1, 1, 1, 1}};
    cudf::test::fixed_width_column_wrapper<T> col2{{10, 40, 70, 5, 2, 10, 11}, {1, 1, 1, 1, 1, 1, 1}};

    CudfTableView cudf_table_in_view {{col1, col2}};

    std::vector<std::string> names({"A", "B"});
    ral::frame::BlazingTableView table(cudf_table_in_view, names);

    std::unique_ptr<ral::frame::BlazingTable> table_out = ral::operators::experimental::logicalLimit(table, 5);

    cudf::test::fixed_width_column_wrapper<T> expect_col1{{5, 4, 3, 5, 8}};
    cudf::test::fixed_width_column_wrapper<T> expect_col2{{10, 40, 70, 5, 2}};
    CudfTableView expect_cudf_table_view {{expect_col1, expect_col2}};

    cudf::test::expect_tables_equal(expect_cudf_table_view, table_out->view());
}