/*
 * Copyright (c) 2019, NVIDIA CORPORATION.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <cudf/cudf.h>
#include <cudf/types.hpp>
#include <from_cudf/cpp_tests/utilities/base_fixture.hpp>
#include <cudf/table/table_view.hpp>
#include <cudf/sorting.hpp>
#include <cudf/copying.hpp>
#include <cudf/column/column_factories.hpp>
#include <from_cudf/cpp_tests/utilities/column_utilities.hpp>
#include <cudf/utilities/type_dispatcher.hpp>
#include <from_cudf/cpp_tests/utilities/type_lists.hpp>
#include <from_cudf/cpp_tests/utilities/column_wrapper.hpp>
#include <from_cudf/cpp_tests/utilities/legacy/cudf_test_utils.cuh>
#include <from_cudf/cpp_tests/utilities/table_utilities.hpp>
#include <vector>
#include <execution_graph/logic_controllers/LogicalFilter.h>
#include <execution_graph/logic_controllers/LogicPrimitives.h>
#include "cudf/stream_compaction.hpp"

using namespace ral::frame;
using namespace ral::processor;

template <typename T>
struct ApplyFilter : public cudf::test::BaseFixture {};

TYPED_TEST_CASE(ApplyFilter, cudf::test::NumericTypes);

TYPED_TEST(ApplyFilter, withNull)
{
    using T = TypeParam;
    cudf::test::fixed_width_column_wrapper<T> col1{{5, 4, 3, 5, 8, 5, 6}, {1, 1, 0, 1, 1, 1, 1}};
    cudf::test::strings_column_wrapper col2({"d", "e", "a", "d", "k", "d", "l"}, {1, 0, 1, 1, 1, 1, 1});
    cudf::test::fixed_width_column_wrapper<T> col3{{10, 40, 70, 5, 2, 10, 11}, {0, 1, 1, 1, 1, 1, 0}};
    CudfTableView cudf_table_in_view {{col1, col2, col3}};
    std::vector<std::string> names({"A", "B", "C"});
    BlazingTableView table_in(cudf_table_in_view, names);
    

    cudf::test::fixed_width_column_wrapper<cudf::experimental::bool8> bool_filter{{1, 1, 1, 0, 1, 0, 0}, {1, 1, 0, 1, 1, 1, 0}};
    cudf::column_view bool_filter_col(bool_filter);

    std::unique_ptr<ral::frame::BlazingTable> table_out = applyBooleanFilter(
        table_in,bool_filter_col);

    cudf::test::fixed_width_column_wrapper<T> expect_col1{{5, 4, 8}, {1, 1,1}};
    cudf::test::strings_column_wrapper expect_col2({"d", "e", "k"}, {1, 0, 1});
    cudf::test::fixed_width_column_wrapper<T> expect_col3{{10, 40, 2}, {0, 1, 1}};
    CudfTableView expect_cudf_table_view {{expect_col1, expect_col2, expect_col3}};

    std::string col0_string = cudf::test::to_string(table_out->view().column(0), "|");
    std::cout<<"col0_string: "<<col0_string<<std::endl;
    std::string col1_string = cudf::test::to_string(table_out->view().column(1), "|");
    std::cout<<"col1_string: "<<col1_string<<std::endl;
    std::string col2_string = cudf::test::to_string(table_out->view().column(2), "|");
    std::cout<<"col2_string: "<<col2_string<<std::endl;

    cudf::test::expect_tables_equal(expect_cudf_table_view, table_out->view());
}

// currently not passing due to issue in apply_boolean_mask. (https://github.com/rapidsai/cudf/issues/3714)
// TYPED_TEST(ApplyFilter, withOutNull)
// {
//     using T = TypeParam;
//     cudf::test::fixed_width_column_wrapper<T> col1{{5, 4, 3, 5, 8, 5, 6}};
//     cudf::test::strings_column_wrapper col2({"d", "e", "a", "d", "k", "d", "l"});
//     cudf::test::fixed_width_column_wrapper<T> col3{{10, 40, 70, 5, 2, 10, 11}};
//     CudfTableView cudf_table_in_view {{col1, col2, col3}};
//     std::vector<std::string> names({"A", "B", "C"});
//     BlazingTableView table_in(cudf_table_in_view, names);
    

//     cudf::test::fixed_width_column_wrapper<cudf::experimental::bool8> bool_filter{{1, 1, 0, 0, 1, 0, 0}};
//     cudf::column_view bool_filter_col(bool_filter);

//     std::unique_ptr<ral::frame::BlazingTable> table_out = applyBooleanFilter(
//         table_in,bool_filter_col);

//     cudf::test::fixed_width_column_wrapper<T> expect_col1{{5, 4, 8}};
//     cudf::test::strings_column_wrapper expect_col2({"d", "e", "k"});
//     cudf::test::fixed_width_column_wrapper<T> expect_col3{{10, 40, 2}};
//     CudfTableView expect_cudf_table_view {{expect_col1, expect_col2, expect_col3}};

//     std::string col0_string = cudf::test::to_string(table_out->view().column(0), "|");
//     std::cout<<"col0_string: "<<col0_string<<std::endl;
//     std::string col1_string = cudf::test::to_string(table_out->view().column(1), "|");
//     std::cout<<"col1_string: "<<col1_string<<std::endl;
//     std::string col2_string = cudf::test::to_string(table_out->view().column(2), "|");
//     std::cout<<"col2_string: "<<col2_string<<std::endl;

//     cudf::test::expect_tables_equal(expect_cudf_table_view, table_out->view());
// }

TYPED_TEST(ApplyFilter, withAndWithOutNull)
{
    using T = TypeParam;
    cudf::test::fixed_width_column_wrapper<T> col1{{5, 4, 3, 5, 8, 5, 6}, {1, 1, 0, 1, 1, 1, 1}};
    cudf::test::strings_column_wrapper col2({"d", "e", "a", "d", "k", "d", "l"}, {1, 0, 1, 1, 1, 1, 1});
    cudf::test::fixed_width_column_wrapper<T> col3{{10, 40, 70, 5, 2, 10, 11}, {0, 1, 1, 1, 1, 1, 0}};
    CudfTableView cudf_table_in_view {{col1, col2, col3}};
    std::vector<std::string> names({"A", "B", "C"});
    BlazingTableView table_in(cudf_table_in_view, names);
    

    cudf::test::fixed_width_column_wrapper<cudf::experimental::bool8> bool_filter{{1, 1, 0, 0, 1, 0, 0}};
    cudf::column_view bool_filter_col(bool_filter);

    std::unique_ptr<ral::frame::BlazingTable> table_out = applyBooleanFilter(
        table_in,bool_filter_col);

    cudf::test::fixed_width_column_wrapper<T> expect_col1{{5, 4, 8}, {1, 1,1}};
    cudf::test::strings_column_wrapper expect_col2({"d", "e", "k"}, {1, 0, 1});
    cudf::test::fixed_width_column_wrapper<T> expect_col3{{10, 40, 2}, {0, 1, 1}};
    CudfTableView expect_cudf_table_view {{expect_col1, expect_col2, expect_col3}};

    std::string col0_string = cudf::test::to_string(table_out->view().column(0), "|");
    std::cout<<"col0_string: "<<col0_string<<std::endl;
    std::string col1_string = cudf::test::to_string(table_out->view().column(1), "|");
    std::cout<<"col1_string: "<<col1_string<<std::endl;
    std::string col2_string = cudf::test::to_string(table_out->view().column(2), "|");
    std::cout<<"col2_string: "<<col2_string<<std::endl;

    cudf::test::expect_tables_equal(expect_cudf_table_view, table_out->view());
}
