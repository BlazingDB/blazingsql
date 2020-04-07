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

template <typename T>
struct InteropsTestNumeric : public cudf::test::BaseFixture {};

TYPED_TEST_CASE(InteropsTestNumeric, cudf::test::NumericTypes);

TYPED_TEST(InteropsTestNumeric, test_numeric_types)
{
  using namespace interops;

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

  std::vector<operator_type> operators = {operator_type::BLZ_ADD, operator_type::BLZ_MUL, operator_type::BLZ_ADD, operator_type::BLZ_ADD};

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

  perform_interpreter_operation(out_table_view,
                              in_table_view,
                              left_inputs,
                              right_inputs,
                              outputs,
                              final_output_positions,
                              operators,
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

template <typename T>
struct InteropsTestNumericDivZero : public cudf::test::BaseFixture {};

TYPED_TEST_CASE(InteropsTestNumericDivZero, cudf::test::NumericTypes);

TYPED_TEST(InteropsTestNumericDivZero, test_numeric_types_divzero)
{
  using namespace interops;

  using T = TypeParam;
  cudf::size_type inputRows = 5;

  auto sequence1 = cudf::test::make_counting_transform_iterator(0, [](auto row) {
      return static_cast<T>(row * 2);
    });
  cudf::test::fixed_width_column_wrapper<T> col1{sequence1, sequence1 + inputRows};

  cudf::table_view in_table_view {{col1}};

  // 0> / $0 0 | / $0 0

  std::vector<column_index_type> left_inputs =  {0};
  std::vector<column_index_type> right_inputs = {-2};
  std::vector<column_index_type> outputs =      {2};

  std::vector<column_index_type> final_output_positions = {2};

  std::vector<operator_type> operators = {operator_type::BLZ_DIV};

  std::unique_ptr<cudf::scalar> arr_s1[] = {nullptr};
  auto dtype = cudf::data_type{cudf::experimental::type_to_id<T>()};
  std::vector<std::unique_ptr<cudf::scalar>> left_scalars(std::make_move_iterator(std::begin(arr_s1)), std::make_move_iterator(std::end(arr_s1)));
  std::unique_ptr<cudf::scalar> arr_s2[] = {cudf::make_numeric_scalar(dtype)};
  std::vector<std::unique_ptr<cudf::scalar>> right_scalars(std::make_move_iterator(std::begin(arr_s2)), std::make_move_iterator(std::end(arr_s2)));
  static_cast<cudf::experimental::scalar_type_t<T>*>(right_scalars[0].get())->set_value((T)0);

  auto sequenceOut = cudf::test::make_counting_transform_iterator(0, [](auto row) {
      return T{};
    });

  auto validity_iter = cudf::test::make_counting_transform_iterator(0,
    [](auto row) { return false; });

  cudf::test::fixed_width_column_wrapper<T> out_col1{sequenceOut, sequenceOut + inputRows, validity_iter};
  cudf::mutable_table_view out_table_view {{out_col1}};

  perform_interpreter_operation(out_table_view,
                              in_table_view,
                              left_inputs,
                              right_inputs,
                              outputs,
                              final_output_positions,
                              operators,
                              left_scalars,
                              right_scalars);

  cudf::test::fixed_width_column_wrapper<T> expected_col1{{0, 0, 0, 0, 0}, {0, 0, 0, 0, 0}};
  cudf::table_view expected_table_view {{expected_col1}};

  cudf::test::expect_tables_equal(expected_table_view, out_table_view);
}

template <typename T>
struct InteropsTestTimestamp : public cudf::test::BaseFixture {};

TYPED_TEST_CASE(InteropsTestTimestamp, cudf::test::TimestampTypes);

TYPED_TEST(InteropsTestTimestamp, test_timestamp_types)
{
  using namespace interops;

  using T = TypeParam;
  cudf::size_type inputRows = 10;

  using Rep = typename T::rep;
  using ToDuration = typename T::duration;

  auto start_ms = cudf::timestamp_ms::duration(-2500000000000);  // Sat, 11 Oct 1890 19:33:20 GMT
  auto start = simt::std::chrono::time_point_cast<ToDuration>(cudf::timestamp_ms(start_ms))
                .time_since_epoch()
                .count();
  auto stop_ms = cudf::timestamp_ms::duration(2500000000000);   // Mon, 22 Mar 2049 04:26:40 GMT
  auto stop = simt::std::chrono::time_point_cast<ToDuration>(cudf::timestamp_ms(stop_ms))
                .time_since_epoch()
                .count();
  auto range = static_cast<Rep>(stop - start);
  auto timestamp_iter = cudf::test::make_counting_transform_iterator(
    0, [=](auto i) { return start + (range / inputRows) * i; });
  cudf::test::fixed_width_column_wrapper<T> col1{timestamp_iter, timestamp_iter + inputRows};
  
  cudf::table_view in_table_view {{col1}};

  std::vector<column_index_type> left_inputs =  {0          , 0          , 0          , 0          , 0          , 0};
  std::vector<column_index_type> right_inputs = {UNARY_INDEX, UNARY_INDEX, UNARY_INDEX, UNARY_INDEX, UNARY_INDEX, UNARY_INDEX};
  std::vector<column_index_type> outputs =      {1          , 2          , 3          , 4          , 5          , 6};

  std::vector<column_index_type> final_output_positions = {1, 2, 3, 4, 5, 6};

	std::vector<operator_type> operators = {operator_type::BLZ_YEAR, operator_type::BLZ_MONTH, operator_type::BLZ_DAY, operator_type::BLZ_HOUR, operator_type::BLZ_MINUTE, operator_type::BLZ_SECOND};

	auto dtype = cudf::data_type{cudf::experimental::type_to_id<T>()};
  std::unique_ptr<cudf::scalar> arr_s1[] = {cudf::make_timestamp_scalar(dtype), cudf::make_timestamp_scalar(dtype), cudf::make_timestamp_scalar(dtype), cudf::make_timestamp_scalar(dtype), cudf::make_timestamp_scalar(dtype), cudf::make_timestamp_scalar(dtype)};
  std::vector<std::unique_ptr<cudf::scalar>> left_scalars(std::make_move_iterator(std::begin(arr_s1)), std::make_move_iterator(std::end(arr_s1)));
  std::unique_ptr<cudf::scalar> arr_s2[] = {cudf::make_timestamp_scalar(dtype), cudf::make_timestamp_scalar(dtype), cudf::make_timestamp_scalar(dtype), cudf::make_timestamp_scalar(dtype), cudf::make_timestamp_scalar(dtype), cudf::make_timestamp_scalar(dtype)};
  std::vector<std::unique_ptr<cudf::scalar>> right_scalars(std::make_move_iterator(std::begin(arr_s2)), std::make_move_iterator(std::end(arr_s2)));
  
  auto sequenceOut = cudf::test::make_counting_transform_iterator(0, [](auto row) {
      return 0;
    });
  cudf::test::fixed_width_column_wrapper<int32_t> out_col1{sequenceOut, sequenceOut + inputRows};
  cudf::test::fixed_width_column_wrapper<int32_t> out_col2{sequenceOut, sequenceOut + inputRows};
  cudf::test::fixed_width_column_wrapper<int32_t> out_col3{sequenceOut, sequenceOut + inputRows};
  cudf::test::fixed_width_column_wrapper<int32_t> out_col4{sequenceOut, sequenceOut + inputRows};
  cudf::test::fixed_width_column_wrapper<int32_t> out_col5{sequenceOut, sequenceOut + inputRows};
  cudf::test::fixed_width_column_wrapper<int32_t> out_col6{sequenceOut, sequenceOut + inputRows};
  cudf::mutable_table_view out_table_view {{out_col1, out_col2, out_col3, out_col4, out_col5, out_col6}};

  perform_interpreter_operation(out_table_view,
                              in_table_view,
                              left_inputs,
                              right_inputs,
                              outputs,
                              final_output_positions,
                              operators,
                              left_scalars,
                              right_scalars);
   
  if (cudf::experimental::type_to_id<T>() == cudf::TIMESTAMP_DAYS){
    cudf::test::fixed_width_column_wrapper<int32_t> expected_col1{{1890,1906,1922,1938,1954,1970,1985,2001,2017,2033}};
    cudf::test::fixed_width_column_wrapper<int32_t> expected_col2{{10,8,6,4,2,1,11,9,7,5}};
    cudf::test::fixed_width_column_wrapper<int32_t> expected_col3{{12,17,21,25,27,1,5,9,14,18}};
    cudf::test::fixed_width_column_wrapper<int32_t> expected_col4{{0,0,0,0,0,0,0,0,0,0}};
    cudf::test::fixed_width_column_wrapper<int32_t> expected_col5{{0,0,0,0,0,0,0,0,0,0}};
    cudf::test::fixed_width_column_wrapper<int32_t> expected_col6{{0,0,0,0,0,0,0,0,0,0}};
    cudf::table_view expected_table_view {{expected_col1, expected_col2, expected_col3, expected_col4, expected_col5, expected_col6}};

    cudf::test::expect_tables_equal(expected_table_view, out_table_view);
  } else {
    cudf::test::fixed_width_column_wrapper<int32_t> expected_col1{{1890,1906,1922,1938,1954,1970,1985,2001,2017,2033}};
    cudf::test::fixed_width_column_wrapper<int32_t> expected_col2{{10,8,6,4,2,1,11,9,7,5}};
    cudf::test::fixed_width_column_wrapper<int32_t> expected_col3{{11,16,20,24,26,1,5,9,14,18}};
    cudf::test::fixed_width_column_wrapper<int32_t> expected_col4{{19,20,21,22,23,0,0,1,2,3}};
    cudf::test::fixed_width_column_wrapper<int32_t> expected_col5{{33,26,20,13,6,0,53,46,40,33}};
    cudf::test::fixed_width_column_wrapper<int32_t> expected_col6{{20,40,0,20,40,0,20,40,0,20}};
    cudf::table_view expected_table_view {{expected_col1, expected_col2, expected_col3, expected_col4, expected_col5, expected_col6}};

    cudf::test::expect_tables_equal(expected_table_view, out_table_view);
  }
}

TYPED_TEST(InteropsTestTimestamp, test_timestamp_comparison)
{
  using namespace interops;

  using T = TypeParam;
  cudf::size_type inputRows = 10;

  using Rep = typename T::rep;
  using ToDuration = typename T::duration;

  auto start_ms = cudf::timestamp_ms::duration(-2500000000000);  // Sat, 11 Oct 1890 19:33:20 GMT
  auto start = simt::std::chrono::time_point_cast<ToDuration>(cudf::timestamp_ms(start_ms))
                .time_since_epoch()
                .count();
  auto stop_ms = cudf::timestamp_ms::duration(2500000000000);   // Mon, 22 Mar 2049 04:26:40 GMT
  auto stop = simt::std::chrono::time_point_cast<ToDuration>(cudf::timestamp_ms(stop_ms))
                .time_since_epoch()
                .count();
  auto range = static_cast<Rep>(stop - start);
  auto timestamp_iter = cudf::test::make_counting_transform_iterator(
    0, [=](auto i) { return start + (range / inputRows) * i; });
  cudf::test::fixed_width_column_wrapper<T> col1{timestamp_iter, timestamp_iter + inputRows};
  
  cudf::table_view in_table_view {{col1}};

  std::vector<column_index_type> left_inputs =  {0          , 0          , 0};
  std::vector<column_index_type> right_inputs = {SCALAR_INDEX, SCALAR_INDEX, SCALAR_INDEX};
  std::vector<column_index_type> outputs =      {1          , 2          , 3};

  std::vector<column_index_type> final_output_positions = {1, 2, 3};

	std::vector<operator_type> operators = {operator_type::BLZ_EQUAL, operator_type::BLZ_LESS, operator_type::BLZ_GREATER_EQUAL};

	auto dtype = cudf::data_type{cudf::experimental::type_to_id<T>()};
  std::unique_ptr<cudf::scalar> arr_s1[] = {cudf::make_timestamp_scalar(dtype), cudf::make_timestamp_scalar(dtype), cudf::make_timestamp_scalar(dtype)};
  std::vector<std::unique_ptr<cudf::scalar>> left_scalars(std::make_move_iterator(std::begin(arr_s1)), std::make_move_iterator(std::end(arr_s1)));
  std::unique_ptr<cudf::scalar> arr_s2[] = {cudf::make_timestamp_scalar(dtype), cudf::make_timestamp_scalar(dtype), cudf::make_timestamp_scalar(dtype)};
  std::vector<std::unique_ptr<cudf::scalar>> right_scalars(std::make_move_iterator(std::begin(arr_s2)), std::make_move_iterator(std::end(arr_s2)));
  static_cast<cudf::experimental::scalar_type_t<T>*>(right_scalars[0].get())->set_value(T{cudf::timestamp_ms{1000000000000}});
  static_cast<cudf::experimental::scalar_type_t<T>*>(right_scalars[1].get())->set_value(T{cudf::timestamp_ms{1000000000000}});
  static_cast<cudf::experimental::scalar_type_t<T>*>(right_scalars[2].get())->set_value(T{cudf::timestamp_ms{1000000000000}});
  
  auto sequenceOut = cudf::test::make_counting_transform_iterator(0, [](auto row) {
      return 0;
    });
  cudf::test::fixed_width_column_wrapper<cudf::experimental::bool8> out_col1{sequenceOut, sequenceOut + inputRows};
  cudf::test::fixed_width_column_wrapper<cudf::experimental::bool8> out_col2{sequenceOut, sequenceOut + inputRows};
  cudf::test::fixed_width_column_wrapper<cudf::experimental::bool8> out_col3{sequenceOut, sequenceOut + inputRows};
  cudf::mutable_table_view out_table_view {{out_col1, out_col2, out_col3}};

  perform_interpreter_operation(out_table_view,
                              in_table_view,
                              left_inputs,
                              right_inputs,
                              outputs,
                              final_output_positions,
                              operators,
                              left_scalars,
                              right_scalars);
   
  cudf::test::fixed_width_column_wrapper<cudf::experimental::bool8> expected_col1{{0,0,0,0,0,0,0,1,0,0}};
  cudf::test::fixed_width_column_wrapper<cudf::experimental::bool8> expected_col2{{1,1,1,1,1,1,1,0,0,0}};
  cudf::test::fixed_width_column_wrapper<cudf::experimental::bool8> expected_col3{{0,0,0,0,0,0,0,1,1,1}};
  cudf::table_view expected_table_view {{expected_col1, expected_col2, expected_col3}};

  cudf::test::expect_tables_equal(expected_table_view, out_table_view);
}

struct InteropsTestString : public cudf::test::BaseFixture {};

TEST_F(InteropsTestString, test_string)
{
  using namespace interops;
  
  cudf::test::strings_column_wrapper col1({"foo", "d", "e", "a", "hello", "k", "d", "l", "bar", ""});
  
  cudf::table_view in_table_view {{col1}};

  std::vector<column_index_type> left_inputs =  {0, 0, SCALAR_INDEX};
  std::vector<column_index_type> right_inputs = {SCALAR_INDEX, SCALAR_INDEX, 0};
  std::vector<column_index_type> outputs =      {1, 2, 3};

  std::vector<column_index_type> final_output_positions = {1, 2, 3};

  std::vector<operator_type> operators = {operator_type::BLZ_EQUAL, operator_type::BLZ_LESS, operator_type::BLZ_GREATER_EQUAL};

  std::unique_ptr<cudf::scalar> arr_s1[] = {nullptr, nullptr, cudf::make_string_scalar("e")};
  std::vector<std::unique_ptr<cudf::scalar>> left_scalars(std::make_move_iterator(std::begin(arr_s1)), std::make_move_iterator(std::end(arr_s1)));
  std::unique_ptr<cudf::scalar> arr_s2[] = {cudf::make_string_scalar("hello"), cudf::make_string_scalar("test"), nullptr};
  std::vector<std::unique_ptr<cudf::scalar>> right_scalars(std::make_move_iterator(std::begin(arr_s2)), std::make_move_iterator(std::end(arr_s2)));
  
  // using OUT_T = typename output_type<T>::type;
  auto sequenceOut = cudf::test::make_counting_transform_iterator(0, [](auto row) {
      return cudf::experimental::bool8{};
    });
  cudf::test::fixed_width_column_wrapper<cudf::experimental::bool8> out_col1{sequenceOut, sequenceOut + 10};
  cudf::test::fixed_width_column_wrapper<cudf::experimental::bool8> out_col2{sequenceOut, sequenceOut + 10};
  cudf::test::fixed_width_column_wrapper<cudf::experimental::bool8> out_col3{sequenceOut, sequenceOut + 10};
  cudf::mutable_table_view out_table_view {{out_col1, out_col2, out_col3}};

  perform_interpreter_operation(out_table_view,
                              in_table_view,
                              left_inputs,
                              right_inputs,
                              outputs,
                              final_output_positions,
                              operators,
                              left_scalars,
                              right_scalars);
  
  cudf::test::fixed_width_column_wrapper<cudf::experimental::bool8> expected_col1{{0,0,0,0,1,0,0,0,0,0}};
  cudf::test::fixed_width_column_wrapper<cudf::experimental::bool8> expected_col2{{1,1,1,1,1,1,1,1,1,1}};
  cudf::test::fixed_width_column_wrapper<cudf::experimental::bool8> expected_col3{{0,1,1,1,0,0,1,0,1,1}};
  cudf::table_view expected_table_view {{expected_col1, expected_col2, expected_col3}};

  cudf::test::expect_tables_equal(expected_table_view, out_table_view);
}
