#include <cstdlib>
#include <iostream>
#include <string>
#include <vector>

#include <execution_graph/logic_controllers/execution_kernels/LogicPrimitives.h>

#include <cudf_test/column_wrapper.hpp>
#include <cudf_test/base_fixture.hpp>
#include <cudf_test/type_lists.hpp>
#include <cudf_test/table_utilities.hpp>
#include <cudf/detail/gather.hpp>
#include "tests/utilities/BlazingUnitTest.h"
#include <operators/OrderBy.h>
#include <utilities/CommonOperations.h>

template <typename T>
struct SortTest : public BlazingUnitTest {};

TYPED_TEST_CASE(SortTest, cudf::test::NumericTypes);

TYPED_TEST(SortTest, withoutNull) {

    using T = TypeParam;

    std::vector<cudf::column_view> rawCols;
    
    cudf::test::fixed_width_column_wrapper<T> col1{{4, 5, 3, 5, 8, 5, 6}, {1, 1, 1, 1, 1, 1, 1}};
    cudf::test::strings_column_wrapper col2({"b", "d", "a", "d", "l", "d", "k"}, {1, 1, 1, 1, 1, 1, 1});
    cudf::test::fixed_width_column_wrapper<T> col3{{10, 40, 70, 5, 2, 10, 11}, {1, 1, 1, 1, 1, 1, 1}};

    CudfTableView cudf_table_in_view {{col1, col2, col3}};

    std::vector<std::string> names({"A", "B", "C"});
    ral::frame::BlazingTableView table(cudf_table_in_view, names);

    std::vector<int> sortColIndices{0,1};
    std::vector<cudf::order> sortOrderTypes{cudf::order::ASCENDING, cudf::order::ASCENDING};

    std::unique_ptr<ral::frame::BlazingTable> table_out = ral::operators::logicalSort(table, sortColIndices, sortOrderTypes);

    cudf::test::fixed_width_column_wrapper<T> expect_col1{{3, 4, 5, 5, 5, 6 ,8}, {1, 1, 1, 1, 1, 1, 1}};
    cudf::test::strings_column_wrapper expect_col2({"a", "b", "d", "d", "d", "k", "l"}, {1, 1, 1, 1, 1, 1, 1});
    cudf::test::fixed_width_column_wrapper<T> expect_col3{{70, 10, 40, 5, 10, 11, 2}, {1, 1, 1, 1, 1, 1, 1}};
    CudfTableView expect_cudf_table_view {{expect_col1, expect_col2, expect_col3}};

    cudf::test::expect_tables_equivalent(expect_cudf_table_view, table_out->view());
}

template <typename T>
struct LimitTest : public BlazingUnitTest {};

TYPED_TEST_CASE(LimitTest, cudf::test::NumericTypes);

TYPED_TEST(LimitTest, withoutNull) {

    using T = TypeParam;

    std::vector<cudf::column_view> rawCols;
    
    cudf::test::fixed_width_column_wrapper<T> col1{{5, 4, 3, 5, 8, 5, 6}, {1, 1, 1, 1, 1, 1, 1}};
    cudf::test::fixed_width_column_wrapper<T> col2{{10, 40, 70, 5, 2, 10, 11}, {1, 1, 1, 1, 1, 1, 1}};

    CudfTableView cudf_table_in_view {{col1, col2}};

    std::vector<std::string> col_names = {"col1","col2"};

    std::unique_ptr<ral::frame::BlazingTable> table_in = std::make_unique<ral::frame::BlazingTable>(cudf_table_in_view, col_names);

    std::unique_ptr<ral::frame::BlazingTable> table_out = ral::utilities::getLimitedRows(table_in->toBlazingTableView(), 5);

    cudf::test::fixed_width_column_wrapper<T> expect_col1{{5, 4, 3, 5, 8}};
    cudf::test::fixed_width_column_wrapper<T> expect_col2{{10, 40, 70, 5, 2}};
    CudfTableView expect_cudf_table_view {{expect_col1, expect_col2}};

    cudf::test::expect_tables_equivalent(expect_cudf_table_view, table_out->view());
}
