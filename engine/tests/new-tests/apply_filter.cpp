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

#include <tests/utilities/base_fixture.hpp>
#include <cudf/table/table_view.hpp>
#include <cudf/sorting.hpp>
#include <cudf/copying.hpp>
#include <cudf/column/column_factories.hpp>
#include <tests/utilities/column_utilities.hpp>
#include <tests/utilities/type_lists.hpp>
#include <tests/utilities/column_wrapper.hpp>
#include <tests/utilities/table_utilities.hpp>
#include <execution_graph/logic_controllers/LogicalFilter.h>
#include "cudf/stream_compaction.hpp"
#include <cudf/datetime.hpp>
#include "tests/utilities/BlazingUnitTest.h"

using namespace ral::frame;
using namespace ral::processor;


template <typename T>
struct ApplyFilter : public BlazingUnitTest {};

// TYPED_TEST_CASE will run all TYPED_TEST with the same name (i.e. ApplyFilter) for all the types specified
// Here the types specified are defined by cudf::test::NumericTypes
// using NumericTypes = cudf::test::Types<int8_t, int16_t, int32_t, int64_t, float, double, bool>;
TYPED_TEST_CASE(ApplyFilter, cudf::test::NumericTypes);


TYPED_TEST(ApplyFilter, withNull)
{
    // TypeParam is the type defined by TYPED_TEST_CASE for a particular test run. Here we are giving TypeParam an alias T
    using T = TypeParam;

    // Here we are creating the input data
    // First we creating three columns using wrapper utilities
    cudf::test::fixed_width_column_wrapper<T> col1({5, 4, 3, 5, 8, 5, 6}, {1, 1, 0, 1, 1, 1, 1});
    cudf::test::strings_column_wrapper col2({"d", "e", "a", "d", "k", "d", "l"}, {1, 0, 1, 1, 1, 1, 1});
    cudf::test::fixed_width_column_wrapper<T> col3({10, 40, 70, 5, 2, 10, 11}, {0, 1, 1, 1, 1, 1, 0});
    // Then we create a CudfTableView from the column wrappers we created
    CudfTableView cudf_table_in_view ({col1, col2, col3});
    // Then using a vector of names and the CudfTableView we are able to create a BlazingTableView
    std::vector<std::string> names({"A", "B", "C"});
    BlazingTableView table_in(cudf_table_in_view, names);    

    // Here we are creating the other input, using a column wrapper and using that to create a column_view
    cudf::test::fixed_width_column_wrapper<bool> bool_filter({1, 1, 1, 0, 1, 0, 0}, {1, 1, 0, 1, 1, 1, 0});
    cudf::column_view bool_filter_col(bool_filter);

    // this is the function under test
    std::unique_ptr<BlazingTable> table_out = applyBooleanFilter(
        table_in,bool_filter_col);

    // Here we are creating the expected output
    cudf::test::fixed_width_column_wrapper<T> expect_col1({5, 4, 8}, {1, 1,1});
    cudf::test::strings_column_wrapper expect_col2({"d", "e", "k"}, {1, 0, 1});
    cudf::test::fixed_width_column_wrapper<T> expect_col3({10, 40, 2}, {0, 1, 1});
    CudfTableView expect_cudf_table_view ({expect_col1, expect_col2, expect_col3});

    // Here we are printing out the output we got (this is optional and only necessary for debugging)
    std::string col0_string = cudf::test::to_string(table_out->view().column(0), "|");
    std::cout<<"col0_string: "<<col0_string<<std::endl;
    std::string col1_string = cudf::test::to_string(table_out->view().column(1), "|");
    std::cout<<"col1_string: "<<col1_string<<std::endl;
    std::string col2_string = cudf::test::to_string(table_out->view().column(2), "|");
    std::cout<<"col2_string: "<<col2_string<<std::endl;

    // Here we are validating the output
    cudf::test::expect_tables_equal(expect_cudf_table_view, table_out->view());
}

TYPED_TEST(ApplyFilter, withOutNull)
{
    using T = TypeParam;
    cudf::test::fixed_width_column_wrapper<T> col1({5, 4, 3, 5, 8, 5, 6});
    cudf::test::strings_column_wrapper col2({"d", "e", "a", "d", "k", "d", "l"});
    cudf::test::fixed_width_column_wrapper<T> col3({10, 40, 70, 5, 2, 10, 11});
    CudfTableView cudf_table_in_view ({col1, col2, col3});
    std::vector<std::string> names({"A", "B", "C"});
    BlazingTableView table_in(cudf_table_in_view, names);
    

    cudf::test::fixed_width_column_wrapper<bool> bool_filter({1, 1, 0, 0, 1, 0, 0});
    cudf::column_view bool_filter_col(bool_filter);

    std::unique_ptr<BlazingTable> table_out = applyBooleanFilter(
        table_in,bool_filter_col);
    
    cudf::test::fixed_width_column_wrapper<T> expect_col1({5, 4, 8});
    cudf::test::strings_column_wrapper expect_col2({"d", "e", "k"});
    cudf::test::fixed_width_column_wrapper<T> expect_col3({10, 40, 2});
    CudfTableView expect_cudf_table_view ({expect_col1, expect_col2, expect_col3});

    // std::string col0_string = cudf::test::to_string(table_out->view().column(0), "|");
    // std::cout<<"col0_string: "<<col0_string<<std::endl;
    // std::string col1_string = cudf::test::to_string(table_out->view().column(1), "|");
    // std::cout<<"col1_string: "<<col1_string<<std::endl;
    // std::string col2_string = cudf::test::to_string(table_out->view().column(2), "|");
    // std::cout<<"col2_string: "<<col2_string<<std::endl;

    cudf::test::expect_tables_equal(expect_cudf_table_view, table_out->view());
}

TYPED_TEST(ApplyFilter, withAndWithOutNull)
{
    using T = TypeParam;
    cudf::test::fixed_width_column_wrapper<T> col1({5, 4, 3, 5, 8, 5, 6}, {1, 1, 0, 1, 1, 1, 1});
    cudf::test::strings_column_wrapper col2({"d", "e", "a", "d", "k", "d", "l"}, {1, 0, 1, 1, 1, 1, 1});
    cudf::test::fixed_width_column_wrapper<T> col3({10, 40, 70, 5, 2, 10, 11}, {0, 1, 1, 1, 1, 1, 0});
    CudfTableView cudf_table_in_view ({col1, col2, col3});
    std::vector<std::string> names({"A", "B", "C"});
    BlazingTableView table_in(cudf_table_in_view, names);
    

    cudf::test::fixed_width_column_wrapper<bool> bool_filter({1, 1, 0, 0, 1, 0, 0});
    cudf::column_view bool_filter_col(bool_filter);

    std::unique_ptr<BlazingTable> table_out = applyBooleanFilter(
        table_in,bool_filter_col);

    cudf::test::fixed_width_column_wrapper<T> expect_col1({5, 4, 8}, {1, 1,1});
    cudf::test::strings_column_wrapper expect_col2({"d", "e", "k"}, {1, 0, 1});
    cudf::test::fixed_width_column_wrapper<T> expect_col3({10, 40, 2}, {0, 1, 1});
    CudfTableView expect_cudf_table_view ({expect_col1, expect_col2, expect_col3});

    std::string col0_string = cudf::test::to_string(table_out->view().column(0), "|");
    std::cout<<"col0_string: "<<col0_string<<std::endl;
    std::string col1_string = cudf::test::to_string(table_out->view().column(1), "|");
    std::cout<<"col1_string: "<<col1_string<<std::endl;
    std::string col2_string = cudf::test::to_string(table_out->view().column(2), "|");
    std::cout<<"col2_string: "<<col2_string<<std::endl;

    cudf::test::expect_tables_equal(expect_cudf_table_view, table_out->view());
}


using namespace cudf::datetime;
using namespace simt::std::chrono;
  
template <typename T>
struct ApplyFilterDates : public BlazingUnitTest {};

// This TYPED_TEST_CASE will run for all cudf::test::TimestampTypes
// using TimestampTypes = cudf::test::Types<timestamp_D, timestamp_s, timestamp_ms, timestamp_us, timestamp_ns>;
TYPED_TEST_CASE(ApplyFilterDates, cudf::test::TimestampTypes);


TYPED_TEST(ApplyFilterDates, interatorWithNull)
{
    // Here we are defining some aliases for datetime specific types 
    using T = TypeParam;
    using Rep = typename T::rep;
    using Period = typename T::period;
    using ToDuration = simt::std::chrono::duration<Rep, Period>;
    using time_point_ms =
    simt::std::chrono::time_point<simt::std::chrono::system_clock,
                                  simt::std::chrono::milliseconds>;

    // Here we are going to create our timestamp column for this test by using an iterator
    // The iterator is a transform iterator that will take a start timestamp and end timestamp
    // and generate timestamps that are equally spaced between the start and stop
    auto start_ms = milliseconds(-2500000000000);  // Sat, 11 Oct 1890 19:33:20 GMT
    auto start = simt::std::chrono::time_point_cast<ToDuration>(time_point_ms(start_ms))
                 .time_since_epoch()
                 .count();
    auto stop_ms = milliseconds(2500000000000);   // Mon, 22 Mar 2049 04:26:40 GMT
    auto stop = simt::std::chrono::time_point_cast<ToDuration>(time_point_ms(stop_ms))
                 .time_since_epoch()
                 .count();
    int32_t size = 10;
    auto range = static_cast<Rep>(stop - start);
    auto timestamp_iter = cudf::test::make_counting_transform_iterator(
      0, [=](auto i) { return start + (range / size) * i; });
    
    // this is the iterator for the valids
    auto valids_iter = cudf::test::make_counting_transform_iterator(0, [](auto i) { return i%2==0? true:false; });

    // now we can create the column wrapper using the iterators
    cudf::test::fixed_width_column_wrapper<T> timestamp_col(timestamp_iter, timestamp_iter + size, valids_iter);    
    
    // this is an iterator for creating another column, but made from int32
    auto int32_iter = cudf::test::make_counting_transform_iterator(100, [](auto i) { return int32_t(i * 2);});
    // this now is my column of int32 but generated using iterators
    cudf::test::fixed_width_column_wrapper<int32_t> int32_col(int32_iter, int32_iter + size, valids_iter);
    CudfTableView cudf_table_in_view ({timestamp_col, int32_col});
    std::vector<std::string> names({"A", "B"});
    BlazingTableView table_in(cudf_table_in_view, names);
    
    // Here we are creating our second input
    cudf::test::fixed_width_column_wrapper<bool> bool_filter({1, 1, 1, 0, 1, 0, 0, 1, 0, 0}, {1, 1, 1, 1, 1, 1, 1, 1, 1, 1});
    cudf::column_view bool_filter_col(bool_filter);

    // This is the function under test
    std::unique_ptr<BlazingTable> table_out = applyBooleanFilter(table_in,bool_filter_col);

    // Here we are going to create our expected output by using a vector of specific values in ms
    // Then we will create an iterator that takes in that vector and converts it into the acutal timestamp datatypes
    std::vector<int64_t> expect_timestamp_ms{-2500000000000,-2000000000000, -1500000000000, -500000000000, 1000000000000};
    auto expect_timestamp_iter = cudf::test::make_counting_transform_iterator(0, [expect_timestamp_ms](auto i){ 
                return simt::std::chrono::time_point_cast<ToDuration>(time_point_ms(milliseconds(expect_timestamp_ms[i])))
                 .time_since_epoch()
                 .count();});
    // Since we cant create a column wrapper using an iterator and a set of literal values, i will also create an iterator for the valids, 
    // but an iterator that uses a vector of valids
    std::vector<bool> expect_valids_vect{1, 0, 1, 1, 0};
    auto expect_valids_iter = cudf::test::make_counting_transform_iterator(0, [expect_valids_vect](auto i){ return expect_valids_vect[i];});
    cudf::test::fixed_width_column_wrapper<T> expect_col1(expect_timestamp_iter, expect_timestamp_iter + expect_timestamp_ms.size(), expect_valids_iter);
    // this other column i can just create from vectors
    cudf::test::fixed_width_column_wrapper<int32_t> expect_col2({200, 202, 204, 208, 214}, {1, 0, 1, 1, 0});
    // those two columns become my expect_cudf_table_view
    CudfTableView expect_cudf_table_view ({expect_col1, expect_col2});

    std::string col0_string = cudf::test::to_string(table_out->view().column(0), "|");
    std::cout<<"col0_string: "<<col0_string<<std::endl;
    std::string col1_string = cudf::test::to_string(table_out->view().column(1), "|");
    std::cout<<"col1_string: "<<col1_string<<std::endl;    

    cudf::test::expect_tables_equal(expect_cudf_table_view, table_out->view());
}
