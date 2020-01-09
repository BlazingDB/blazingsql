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

#include <cudf/column/column_factories.hpp>
#include <cudf/copying.hpp>
#include <cudf/cudf.h>
#include <cudf/datetime.hpp>
#include <cudf/sorting.hpp>
#include <cudf/scalar/scalar_factories.hpp>
#include <cudf/table/table_view.hpp>
#include <cudf/types.hpp>
#include <cudf/utilities/type_dispatcher.hpp>
#include <vector>

#include "from_cudf/cpp_tests/utilities/base_fixture.hpp"
#include "from_cudf/cpp_tests/utilities/column_utilities.hpp"
#include "from_cudf/cpp_tests/utilities/column_wrapper.hpp"
#include "from_cudf/cpp_tests/utilities/legacy/cudf_test_utils.cuh"
#include "from_cudf/cpp_tests/utilities/table_utilities.hpp"
#include "from_cudf/cpp_tests/utilities/type_lists.hpp"
#include "Interpreter/interpreter_cpp.h"

template<typename T, typename Enabled = void>
struct output_type {};

template<typename T>
struct output_type<T, typename std::enable_if<!std::is_floating_point<T>::value>::type> {
    using type = int64_t;
};

template<typename T>
struct output_type<T, typename std::enable_if<std::is_floating_point<T>::value>::type> {
    using type = double;
};

template <typename T>
struct InteropsTest : public cudf::test::BaseFixture {};

TYPED_TEST_CASE(InteropsTest, cudf::test::NumericTypes);
// using TestTypes = cudf::test::Types<cudf::experimental::bool8>;
// TYPED_TEST_CASE(InteropsTest, TestTypes);

TYPED_TEST(InteropsTest, test_numeric_types)
{
    using T = TypeParam;
    cudf::size_type inputRows = 10;

    auto sequence1 = cudf::test::make_counting_transform_iterator(0, [](auto row) {
          return static_cast<T>(row * 2);
      });
    cudf::test::fixed_width_column_wrapper<T> col1{sequence1, sequence1 + inputRows};

    auto sequence2 = cudf::test::make_counting_transform_iterator(0, [](auto row) {
          return static_cast<T>(row * 10);
      });
    cudf::test::fixed_width_column_wrapper<T> col2{sequence2, sequence2 + inputRows};

    auto sequence3 = cudf::test::make_counting_transform_iterator(0, [](auto row) {
          return static_cast<T>(row * 20);
      });
    cudf::test::fixed_width_column_wrapper<T> col3{sequence3, sequence3 + inputRows};
    
    cudf::table_view in_table_view {{col1, col2, col3}};

    // 0> + * + $0 $1 $2 $1   | + $1 2
    // 1> + * (+ $0 $1) $2 $1 | + $1 2
    // 2> + (* $5 $2) $1      | + $1 2
    // 3> (+ $5 $1)           | (+ $1 2)
    // 4> $3                  | $4

    std::vector<column_index_type> left_inputs =  {0, 5, 5,      1};
    std::vector<column_index_type> right_inputs = {1, 2, 1,      SCALAR_INDEX};
    std::vector<column_index_type> outputs =      {5, 5, 3,      4};

    std::vector<column_index_type> final_output_positions = {3, 4};

    std::vector<gdf_binary_operator_exp> operators = {BLZ_ADD, BLZ_MUL, BLZ_ADD, BLZ_ADD};
    std::vector<gdf_unary_operator> unary_operators = {BLZ_INVALID_UNARY, BLZ_INVALID_UNARY, BLZ_INVALID_UNARY, BLZ_INVALID_UNARY};

    auto dtype = cudf::data_type{cudf::experimental::type_to_id<T>()};
    std::unique_ptr<cudf::scalar> arr_s1[] = {cudf::make_numeric_scalar(dtype), cudf::make_numeric_scalar(dtype), cudf::make_numeric_scalar(dtype), cudf::make_numeric_scalar(dtype)};
    std::vector<std::unique_ptr<cudf::scalar>> left_scalars(std::make_move_iterator(std::begin(arr_s1)), std::make_move_iterator(std::end(arr_s1)));
    std::unique_ptr<cudf::scalar> arr_s2[] = {cudf::make_numeric_scalar(dtype), cudf::make_numeric_scalar(dtype), cudf::make_numeric_scalar(dtype), cudf::make_numeric_scalar(dtype)};
    std::vector<std::unique_ptr<cudf::scalar>> right_scalars(std::make_move_iterator(std::begin(arr_s2)), std::make_move_iterator(std::end(arr_s2)));
    static_cast<cudf::experimental::scalar_type_t<T>*>(right_scalars[3].get())->set_value((T)2);
  
    // using OUT_T = typename output_type<T>::type;
    auto sequenceOut = cudf::test::make_counting_transform_iterator(0, [](auto row) {
          return T{};
      });
    cudf::test::fixed_width_column_wrapper<T> out_col1{sequenceOut, sequenceOut + inputRows};
    cudf::test::fixed_width_column_wrapper<T> out_col2{sequenceOut, sequenceOut + inputRows};
    cudf::mutable_table_view out_table_view {{out_col1, out_col2}};

    perform_operation(out_table_view,
        in_table_view,
        left_inputs,
        right_inputs,
        outputs,
        final_output_positions,
        operators,
        unary_operators,
        left_scalars,
        right_scalars);

    // for (auto &&c : out_table_view) {
    //     cudf::test::print(c);
    //     std::cout << std::endl;
    // }
    
    cudf::test::fixed_width_column_wrapper<T> expected_col1{{(T)0, (T)250, (T)980, (T)2190, (T)3880, (T)6050, (T)8700, (T)11830, (T)15440, (T)19530}};
    cudf::test::fixed_width_column_wrapper<T> expected_col2{{(T)2, (T)12, (T)22, (T)32, (T)42, (T)52, (T)62, (T)72, (T)82, (T)92}};
    cudf::table_view expected_table_view {{expected_col1, expected_col2}};

    cudf::test::expect_tables_equal(expected_table_view, out_table_view);
}
