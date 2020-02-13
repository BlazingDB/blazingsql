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
#include "execution_graph/logic_controllers/LogicalFilter.h"
#include "utilities/DebuggingUtils.h"

template <typename T>
struct LogicalFilterTest : public cudf::test::BaseFixture {};

TYPED_TEST_CASE(LogicalFilterTest, cudf::test::NumericTypes);

TYPED_TEST(LogicalFilterTest, filter_input_table_empty)
{
  using namespace ral::frame;

  using T = TypeParam;
  
  cudf::test::fixed_width_column_wrapper<T> col1{};
  
  cudf::table_view in_table_view {{col1}};
  std::vector<std::string> column_names(in_table_view.num_columns());

  auto out_table = ral::processor::process_filter(BlazingTableView{in_table_view, column_names},
                                                  "LogicalFilter(condition=[<(+($0, $1), 0)])",
                                                  nullptr);
  
  cudf::test::fixed_width_column_wrapper<T> expected_col1{};
  cudf::table_view expected_table_view {{expected_col1}};

  cudf::test::expect_tables_equal(expected_table_view, out_table->view());
}

TYPED_TEST(LogicalFilterTest, filter_out_table_empty)
{
  using namespace ral::frame;

  using T = TypeParam;
  cudf::size_type inputRows = 10;

  auto sequence1 = cudf::test::make_counting_transform_iterator(0, [](auto row) {
      return static_cast<T>(row);
    });
  cudf::test::fixed_width_column_wrapper<T> col1{sequence1, sequence1 + inputRows};

  auto sequence2 = cudf::test::make_counting_transform_iterator(0, [](auto row) {
      return static_cast<T>(2 * row);
    });
  cudf::test::fixed_width_column_wrapper<T> col2{sequence2, sequence2 + inputRows};
  
  cudf::table_view in_table_view {{col1, col2}};
  std::vector<std::string> column_names(in_table_view.num_columns());

  auto out_table = ral::processor::process_filter(BlazingTableView{in_table_view, column_names},
                                                  "LogicalFilter(condition=[<(+($0, $1), 0)])",
                                                  nullptr);
  
  cudf::test::fixed_width_column_wrapper<T> expected_col1{};
  cudf::test::fixed_width_column_wrapper<T> expected_col2{};
  cudf::table_view expected_table_view {{expected_col1, expected_col2}};

  cudf::test::expect_tables_equal(expected_table_view, out_table->view());
}

TYPED_TEST(LogicalFilterTest, filter_table)
{
  using namespace ral::frame;

  using T = TypeParam;
  cudf::size_type inputRows = 40;

  auto sequence1 = cudf::test::make_counting_transform_iterator(0, [](auto row) {
      return static_cast<T>(row);
    });
  cudf::test::fixed_width_column_wrapper<T> col1{sequence1, sequence1 + inputRows};

  auto sequence2 = cudf::test::make_counting_transform_iterator(0, [](auto row) {
      return static_cast<T>(2 * row);
    });
  cudf::test::fixed_width_column_wrapper<T> col2{sequence2, sequence2 + inputRows};
  
  cudf::table_view in_table_view {{col1, col2}};
  std::vector<std::string> column_names(in_table_view.num_columns());

  auto out_table = ral::processor::process_filter(BlazingTableView{in_table_view, column_names},
                                                  "LogicalFilter(condition=[=(MOD(+($0, $1), 2), 0)])",
                                                  nullptr);

  cudf::size_type outputRows = (inputRows / 2);
  if (cudf::experimental::type_to_id<T>() == cudf::BOOL8) {
    outputRows = inputRows;
  }  

  auto sequenceOut1 = cudf::test::make_counting_transform_iterator(0, [](auto row) {
      return static_cast<T>(2 * row);;
    });
  cudf::test::fixed_width_column_wrapper<T> expected_col1{sequenceOut1, sequenceOut1 + outputRows};

  auto sequenceOut2 = cudf::test::make_counting_transform_iterator(0, [](auto row) {
      return static_cast<T>(4 * row);;
    });
  cudf::test::fixed_width_column_wrapper<T> expected_col2{sequenceOut2, sequenceOut2 + outputRows};
  cudf::table_view expected_table_view {{expected_col1, expected_col2}};

  cudf::test::expect_tables_equal(expected_table_view, out_table->view());
}

TYPED_TEST(LogicalFilterTest, filter_table_with_nulls)
{
  using namespace ral::frame;

  using T = TypeParam;
  cudf::size_type inputRows = 40;

  auto sequence1 = cudf::test::make_counting_transform_iterator(0, [](auto row) {
      return static_cast<T>(row);
    });
  auto sequenceValidity1 = cudf::test::make_counting_transform_iterator(0, [](auto row) {
      return row % 2 == 0;
    });
  cudf::test::fixed_width_column_wrapper<T> col1{sequence1, sequence1 + inputRows, sequenceValidity1};

  auto sequence2 = cudf::test::make_counting_transform_iterator(0, [](auto row) {
      return static_cast<T>(2 * row);
    });
  auto sequenceValidity2 = cudf::test::make_counting_transform_iterator(0, [](auto row) {
      return row % 2 == 0;
    });
  cudf::test::fixed_width_column_wrapper<T> col2{sequence2, sequence2 + inputRows, sequenceValidity2};
  
  cudf::table_view in_table_view {{col1, col2}};
  std::vector<std::string> column_names(in_table_view.num_columns());

  auto out_table = ral::processor::process_filter(BlazingTableView{in_table_view, column_names},
                                                  "LogicalFilter(condition=[=(MOD(+($0, $1), 2), 0)])",
                                                  nullptr);

  // for (auto &&c : out_table->view()) {
  //   cudf::test::print(c);
  //   std::cout << std::endl;
  // }
  
  auto sequenceOut1 = cudf::test::make_counting_transform_iterator(0, [](auto row) {
      return static_cast<T>(2 * row);;
    });
  auto sequenceOutValidity1 = cudf::test::make_counting_transform_iterator(0, [](auto row) {
      return true;
    });
  cudf::test::fixed_width_column_wrapper<T> expected_col1{sequenceOut1, sequenceOut1 + (inputRows / 2), sequenceOutValidity1};

  auto sequenceOut2 = cudf::test::make_counting_transform_iterator(0, [](auto row) {
      return static_cast<T>(4 * row);;
    });
  auto sequenceOutValidity2 = cudf::test::make_counting_transform_iterator(0, [](auto row) {
      return true;
    });
  cudf::test::fixed_width_column_wrapper<T> expected_col2{sequenceOut2, sequenceOut2 + (inputRows / 2), sequenceOutValidity2};
  cudf::table_view expected_table_view {{expected_col1, expected_col2}};

  cudf::test::expect_tables_equal(expected_table_view, out_table->view());
}


struct LogicalFilterWithStringsTest : public cudf::test::BaseFixture {};

TEST_F(LogicalFilterWithStringsTest, NoNulls)
{
  cudf::test::strings_column_wrapper col1({"foo", "d", "e", "a", "hello", "k", "d", "l", "bar", ""});
  cudf::test::fixed_width_column_wrapper<int32_t> col2{{10,8,6,4,2,1,11,9,7,5}};

  cudf::table_view in_table_view {{col1, col2}};
  std::vector<std::string> column_names{"col1", "col2"};

  auto out_table = ral::processor::process_filter(ral::frame::BlazingTableView{in_table_view, column_names},
                                                  "LogicalFilter(condition=[=($0, 'bar')])", nullptr);
  
  cudf::test::strings_column_wrapper expected_col1({"bar"});
  cudf::test::fixed_width_column_wrapper<int32_t> expected_col2{{7}};

  cudf::table_view expected_table_view {{expected_col1, expected_col2}};

  cudf::test::expect_tables_equal(expected_table_view, out_table->view());
}

TEST_F(LogicalFilterWithStringsTest, IneqWithNulls)
{  
  cudf::test::strings_column_wrapper col1({"foo", "d", "e", "a", "hello", "k", "d", "l", "bar", ""}, {1, 1, 0, 1, 1, 0, 1, 1, 0, 1});
  cudf::test::fixed_width_column_wrapper<int32_t> col2{{10,8,6,4,2,1,11,9,7,5}, {1, 1, 0, 1, 1, 0, 1, 1, 0, 1}};

  cudf::table_view in_table_view {{col1, col2}};
  std::vector<std::string> column_names{"col1", "col2"};

  auto out_table = ral::processor::process_filter(ral::frame::BlazingTableView{in_table_view, column_names},
                                                  "LogicalFilter(condition=[>($0, 'd')])", nullptr);

  cudf::test::strings_column_wrapper expected_col1({"foo", "hello", "l"}, {1, 1, 1});
  cudf::test::fixed_width_column_wrapper<int32_t> expected_col2{{10, 2, 9}, {1, 1, 1}};

  cudf::table_view expected_table_view {{expected_col1, expected_col2}};

  cudf::test::expect_tables_equal(expected_table_view, out_table->view());
}
