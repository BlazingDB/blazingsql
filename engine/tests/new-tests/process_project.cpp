#include "cudf_test/base_fixture.hpp"
#include "cudf_test/column_utilities.hpp"
#include "cudf_test/column_wrapper.hpp"
#include "cudf_test/table_utilities.hpp"
#include "cudf_test/type_lists.hpp"

#include "execution_graph/logic_controllers/LogicalProject.h"

#include <execution_graph/logic_controllers/LogicPrimitives.h>
#include "tests/utilities/BlazingUnitTest.h"

template <typename T>
struct ProjectTestNumeric : public BlazingUnitTest {};

TYPED_TEST_CASE(ProjectTestNumeric, cudf::test::NumericTypes);

TYPED_TEST(ProjectTestNumeric, test_numeric_types1)
{
    using T = TypeParam;

    cudf::test::fixed_width_column_wrapper<T> col1{{4, 5, 3, 5, 8, 5, 6}, {1, 1, 1, 1, 1, 1, 1}};
    cudf::test::strings_column_wrapper col2({"b", "d", "a", "d", "l", "d", "k"}, {1, 1, 1, 1, 1, 1, 1});
    cudf::test::fixed_width_column_wrapper<T> col3{{10, 40, 70, 5, 2, 10, 11}, {1, 1, 1, 1, 1, 1, 1}};

    CudfTableView cudf_table_in_view {{col1, col2, col3}};
    std::unique_ptr<CudfTable> cudf_table = std::make_unique<CudfTable>(cudf_table_in_view);

    std::vector<std::string> names({"A", "B", "C"});
    std::unique_ptr<ral::frame::BlazingTable> table = std::make_unique<ral::frame::BlazingTable>(std::move(cudf_table), names);

    std::string query_part = "LogicalProject(A=[$0])";
    blazingdb::manager::Context * context = nullptr;

    std::unique_ptr<ral::frame::BlazingTable> table_out = ral::processor::process_project(
        std::move(table), 
        query_part,
        context);

    cudf::test::fixed_width_column_wrapper<T> expect_col1{{4, 5, 3, 5, 8, 5, 6}, {1, 1, 1, 1, 1, 1, 1}};
    CudfTableView expect_cudf_table_view {{expect_col1}};

    cudf::test::expect_tables_equal(expect_cudf_table_view, table_out->view());
}

TYPED_TEST(ProjectTestNumeric, test_numeric_types2)
{
    using T = TypeParam;

    cudf::test::fixed_width_column_wrapper<T> col1{{4, 5, 3, 5, 8, 5, 6}, {1, 1, 1, 1, 1, 1, 1}};
    cudf::test::strings_column_wrapper col2({"b", "d", "a", "d", "l", "d", "k"}, {1, 1, 1, 1, 1, 1, 1});
    cudf::test::fixed_width_column_wrapper<T> col3{{10, 40, 70, 5, 2, 10, 11}, {1, 1, 1, 1, 1, 1, 1}};

    CudfTableView cudf_table_in_view {{col1, col2, col3}};
    std::unique_ptr<CudfTable> cudf_table = std::make_unique<CudfTable>(cudf_table_in_view);

    std::vector<std::string> names({"A", "B", "C"});
    std::unique_ptr<ral::frame::BlazingTable> table = std::make_unique<ral::frame::BlazingTable>(std::move(cudf_table), names);

    std::string query_part = "LogicalProject(C=[$2])";
    blazingdb::manager::Context * context = nullptr;

    std::unique_ptr<ral::frame::BlazingTable> table_out = ral::processor::process_project(
        std::move(table), 
        query_part,
        context);

    cudf::test::fixed_width_column_wrapper<T> expect_col3{{10, 40, 70, 5, 2, 10, 11}, {1, 1, 1, 1, 1, 1, 1}};
    CudfTableView expect_cudf_table_view {{expect_col3}};

    cudf::test::expect_tables_equal(expect_cudf_table_view, table_out->view());
}

TYPED_TEST(ProjectTestNumeric, test_numeric_types3)
{
    using T = TypeParam;

    cudf::test::fixed_width_column_wrapper<T> col1{{4, 5, 3, 5, 8, 5, 6}, {1, 1, 1, 1, 1, 1, 1}};
    cudf::test::strings_column_wrapper col2({"b", "d", "a", "d", "l", "d", "k"}, {1, 1, 1, 1, 1, 1, 1});
    cudf::test::fixed_width_column_wrapper<T> col3{{10, 40, 70, 5, 2, 10, 11}, {1, 1, 1, 1, 1, 1, 1}};

    CudfTableView cudf_table_in_view {{col1, col2, col3}};
    std::unique_ptr<CudfTable> cudf_table = std::make_unique<CudfTable>(cudf_table_in_view);

    std::vector<std::string> names({"A", "B", "C"});
    std::unique_ptr<ral::frame::BlazingTable> table = std::make_unique<ral::frame::BlazingTable>(std::move(cudf_table), names);

    std::string query_part = "LogicalProject(A=[$0], C=[$2])";
    blazingdb::manager::Context * context = nullptr;

    std::unique_ptr<ral::frame::BlazingTable> table_out = ral::processor::process_project(
        std::move(table), 
        query_part,
        context);

    cudf::test::fixed_width_column_wrapper<T> expect_col1{{4, 5, 3, 5, 8, 5, 6}, {1, 1, 1, 1, 1, 1, 1}};
    cudf::test::fixed_width_column_wrapper<T> expect_col3{{10, 40, 70, 5, 2, 10, 11}, {1, 1, 1, 1, 1, 1, 1}};
    CudfTableView expect_cudf_table_view {{expect_col1, expect_col3}};

    cudf::test::expect_tables_equal(expect_cudf_table_view, table_out->view());
}

TYPED_TEST(ProjectTestNumeric, test_numeric_types4)
{
    using T = TypeParam;

    cudf::test::fixed_width_column_wrapper<T> col1{{4, 5, 3, 5, 8, 5, 6}, {1, 1, 1, 1, 1, 1, 1}};
    cudf::test::strings_column_wrapper col2({"b", "d", "a", "d", "l", "d", "k"}, {1, 1, 1, 1, 1, 1, 1});
    cudf::test::fixed_width_column_wrapper<T> col3{{10, 40, 70, 5, 2, 10, 11}, {1, 1, 1, 1, 1, 1, 1}};

    CudfTableView cudf_table_in_view {{col1, col2, col3}};
    std::unique_ptr<CudfTable> cudf_table = std::make_unique<CudfTable>(cudf_table_in_view);

    std::vector<std::string> names({"A", "B", "C"});
    std::unique_ptr<ral::frame::BlazingTable> table = std::make_unique<ral::frame::BlazingTable>(std::move(cudf_table), names);

    std::string query_part = "LogicalProject(EXPR$0=[1])";
    blazingdb::manager::Context * context = nullptr;

    std::unique_ptr<ral::frame::BlazingTable> table_out = ral::processor::process_project(
        std::move(table), 
        query_part,
        context);

    cudf::test::fixed_width_column_wrapper<int8_t> expect_col1{{1, 1, 1, 1, 1, 1, 1}};
    CudfTableView expect_cudf_table_view {{expect_col1}};

    cudf::test::expect_tables_equal(expect_cudf_table_view, table_out->view());
}

TYPED_TEST(ProjectTestNumeric, test_numeric_types5)
{
    using T = TypeParam;

    cudf::test::fixed_width_column_wrapper<T> col1{{4, 5, 3, 5, 8, 5, 6}, {1, 1, 1, 1, 1, 1, 1}};
    cudf::test::strings_column_wrapper col2({"b", "d", "a", "d", "l", "d", "k"}, {1, 1, 1, 1, 1, 1, 1});
    cudf::test::fixed_width_column_wrapper<T> col3{{10, 40, 70, 5, 2, 10, 11}, {1, 1, 1, 1, 1, 1, 1}};

    CudfTableView cudf_table_in_view {{col1, col2, col3}};
    std::unique_ptr<CudfTable> cudf_table = std::make_unique<CudfTable>(cudf_table_in_view);

    std::vector<std::string> names({"A", "B", "C"});
    std::unique_ptr<ral::frame::BlazingTable> table = std::make_unique<ral::frame::BlazingTable>(std::move(cudf_table), names);

    std::string query_part = "LogicalProject(EXPR$0=[>($0, 3)])";
    blazingdb::manager::Context * context = nullptr;

    std::unique_ptr<ral::frame::BlazingTable> table_out = ral::processor::process_project(
        std::move(table),
        query_part,
        context);

    if (std::is_same<T, bool>::value) {
        cudf::test::fixed_width_column_wrapper<bool> expect_col1{{0, 0, 0, 0, 0, 0, 0}, {1, 1, 1, 1, 1, 1, 1}};
        CudfTableView expect_cudf_table_view {{expect_col1}};

        cudf::test::expect_tables_equal(expect_cudf_table_view, table_out->view());
    } else {
        cudf::test::fixed_width_column_wrapper<bool> expect_col1{{1, 1, 0, 1, 1, 1, 1}, {1, 1, 1, 1, 1, 1, 1}};
        CudfTableView expect_cudf_table_view {{expect_col1}};
    
        cudf::test::expect_tables_equal(expect_cudf_table_view, table_out->view());
    }
}

TYPED_TEST(ProjectTestNumeric, test_numeric_types6)
{
    using T = TypeParam;

    cudf::test::fixed_width_column_wrapper<T> col1{{4, 5, 3, 5, 8, 5, 6}, {1, 1, 1, 1, 1, 1, 1}};
    cudf::test::strings_column_wrapper col2({"b", "d", "a", "d", "l", "d", "k"}, {1, 1, 1, 1, 1, 1, 1});
    cudf::test::fixed_width_column_wrapper<T> col3{{10, 40, 70, 5, 2, 10, 11}, {1, 1, 1, 1, 1, 1, 1}};

    CudfTableView cudf_table_in_view {{col1, col2, col3}};
    std::unique_ptr<CudfTable> cudf_table = std::make_unique<CudfTable>(cudf_table_in_view);

    std::vector<std::string> names({"A", "B", "C"});
    std::unique_ptr<ral::frame::BlazingTable> table = std::make_unique<ral::frame::BlazingTable>(std::move(cudf_table), names);

    std::string query_part = "LogicalProject(EXPR$0=[>($0, 3)], EXPR$1=[<($2, 3)])";
    blazingdb::manager::Context * context = nullptr;

    std::unique_ptr<ral::frame::BlazingTable> table_out = ral::processor::process_project(
        std::move(table), 
        query_part,
        context);

    if (std::is_same<T, bool>::value) {
        cudf::test::fixed_width_column_wrapper<bool> expect_col1{{0, 0, 0, 0, 0, 0, 0}, {1, 1, 1, 1, 1, 1, 1}};
        cudf::test::fixed_width_column_wrapper<bool> expect_col3{{1, 1, 1, 1, 1, 1, 1}, {1, 1, 1, 1, 1, 1, 1}};
        CudfTableView expect_cudf_table_view {{expect_col1, expect_col3}};

        cudf::test::expect_tables_equal(expect_cudf_table_view, table_out->view());
    } else {
        cudf::test::fixed_width_column_wrapper<bool> expect_col1{{1, 1, 0, 1, 1, 1, 1}, {1, 1, 1, 1, 1, 1, 1}};
        cudf::test::fixed_width_column_wrapper<bool> expect_col3{{0, 0, 0, 0, 1, 0, 0}, {1, 1, 1, 1, 1, 1, 1}};
        CudfTableView expect_cudf_table_view {{expect_col1, expect_col3}};

        cudf::test::expect_tables_equal(expect_cudf_table_view, table_out->view());
    }
}


TYPED_TEST(ProjectTestNumeric, test_numeric_types7)
{
    using T = TypeParam;

    cudf::test::fixed_width_column_wrapper<T> col1{{4, 5, 3, 5, 8, 5, 6}, {1, 1, 1, 1, 1, 1, 1}};
    cudf::test::strings_column_wrapper col2({"b", "d", "a", "d", "l", "d", "k"}, {1, 1, 1, 1, 1, 1, 1});
    cudf::test::fixed_width_column_wrapper<T> col3{{10, 40, 70, 5, 2, 10, 11}, {1, 1, 1, 1, 1, 1, 1}};

    CudfTableView cudf_table_in_view {{col1, col2, col3}};
    std::unique_ptr<CudfTable> cudf_table = std::make_unique<CudfTable>(cudf_table_in_view);

    std::vector<std::string> names({"A", "B", "C"});
    std::unique_ptr<ral::frame::BlazingTable> table = std::make_unique<ral::frame::BlazingTable>(std::move(cudf_table), names);

    std::string query_part = "LogicalProject(EXPR$0=[RAND()], EXPR$1=[*($2, RAND())])";
    blazingdb::manager::Context * context = nullptr;

    std::unique_ptr<ral::frame::BlazingTable> table_out = ral::processor::process_project(
        std::move(table), 
        query_part,
        context);

  //  for (auto &&c : table_out->toBlazingTableView().view()) {
  //     cudf::test::print(c);
  //     std::cout << std::endl;
  // }
    //if (std::is_same<T, bool>::value) {
    //    cudf::test::fixed_width_column_wrapper<bool> expect_col1{{0, 0, 0, 0, 0, 0, 0}, {1, 1, 1, 1, 1, 1, 1}};
    //    cudf::test::fixed_width_column_wrapper<bool> expect_col3{{1, 1, 1, 1, 1, 1, 1}, {1, 1, 1, 1, 1, 1, 1}};
    //    CudfTableView expect_cudf_table_view {{expect_col1, expect_col3}};

//        cudf::test::expect_tables_equal(expect_cudf_table_view, table_out->view());
//    } else {
//        cudf::test::fixed_width_column_wrapper<bool> expect_col1{{1, 1, 0, 1, 1, 1, 1}, {1, 1, 1, 1, 1, 1, 1}};
//        cudf::test::fixed_width_column_wrapper<bool> expect_col3{{0, 0, 0, 0, 1, 0, 0}, {1, 1, 1, 1, 1, 1, 1}};
//        CudfTableView expect_cudf_table_view {{expect_col1, expect_col3}};

//        cudf::test::expect_tables_equal(expect_cudf_table_view, table_out->view());
//   }
}

TYPED_TEST(ProjectTestNumeric, test_rand)
{
    using T = TypeParam;

    cudf::test::fixed_width_column_wrapper<T> col1{{4, 5, 3, 5, 8, 5, 6}, {1, 1, 1, 1, 1, 1, 1}};
    cudf::test::strings_column_wrapper col2({"b", "d", "a", "d", "l", "d", "k"}, {1, 1, 1, 1, 1, 1, 1});
    cudf::test::fixed_width_column_wrapper<T> col3{{10, 40, 70, 5, 2, 10, 11}, {1, 1, 1, 1, 1, 1, 1}};

    CudfTableView cudf_table_in_view {{col1, col2, col3}};
    std::unique_ptr<CudfTable> cudf_table = std::make_unique<CudfTable>(cudf_table_in_view);

    std::vector<std::string> names({"A", "B", "C"});
    std::unique_ptr<ral::frame::BlazingTable> table = std::make_unique<ral::frame::BlazingTable>(std::move(cudf_table), names);

    std::string query_part = "LogicalProject(EXPR$0=[RAND()])";
    blazingdb::manager::Context * context = nullptr;

    std::unique_ptr<ral::frame::BlazingTable> table_out = ral::processor::process_project(
        std::move(table), 
        query_part,
        context);

    for (auto &&c : table_out->toBlazingTableView().view()) {
       cudf::test::print(c);
       std::cout << std::endl;
   }
    //if (std::is_same<T, bool>::value) {
    //    cudf::test::fixed_width_column_wrapper<bool> expect_col1{{0, 0, 0, 0, 0, 0, 0}, {1, 1, 1, 1, 1, 1, 1}};
    //    cudf::test::fixed_width_column_wrapper<bool> expect_col3{{1, 1, 1, 1, 1, 1, 1}, {1, 1, 1, 1, 1, 1, 1}};
    //    CudfTableView expect_cudf_table_view {{expect_col1, expect_col3}};

//        cudf::test::expect_tables_equal(expect_cudf_table_view, table_out->view());
//    } else {
//        cudf::test::fixed_width_column_wrapper<bool> expect_col1{{1, 1, 0, 1, 1, 1, 1}, {1, 1, 1, 1, 1, 1, 1}};
//        cudf::test::fixed_width_column_wrapper<bool> expect_col3{{0, 0, 0, 0, 1, 0, 0}, {1, 1, 1, 1, 1, 1, 1}};
//        CudfTableView expect_cudf_table_view {{expect_col1, expect_col3}};

//        cudf::test::expect_tables_equal(expect_cudf_table_view, table_out->view());
//   }
}


struct ProjectTestString : public BlazingUnitTest {};

TEST_F(ProjectTestString, test_string_like)
{
    cudf::test::strings_column_wrapper col1{{"foo", "d", "e", "a", "hello", "k", "d", "l", "bar", ""}};
  
    cudf::table_view in_table_view {{col1}};
    std::unique_ptr<CudfTable> cudf_table = std::make_unique<CudfTable>(in_table_view);
    std::vector<std::string> names(in_table_view.num_columns());
    std::unique_ptr<ral::frame::BlazingTable> table = std::make_unique<ral::frame::BlazingTable>(std::move(cudf_table), names);

    auto out_table = ral::processor::process_project(std::move(table),
                                                    "LogicalProject(EXPR$0=[LIKE($0, '_')], EXPR$1=[LIKE($0, '%o')])",
                                                    nullptr);
  
    cudf::test::fixed_width_column_wrapper<bool> expected_col1{{0,1,1,1,0,1,1,1,0,0}};
    cudf::test::fixed_width_column_wrapper<bool> expected_col2{{1,0,0,0,1,0,0,0,0,0}};
    cudf::table_view expected_table_view {{expected_col1, expected_col2}};

    cudf::test::expect_tables_equal(expected_table_view, out_table->view());
}

TEST_F(ProjectTestString, test_string_substring)
{
    cudf::test::strings_column_wrapper col1{{"The", "quick", "brown", "fox", "jumps", "over", "the", "lazy", "dog"}};
  
    cudf::table_view in_table_view {{col1}};
    std::unique_ptr<CudfTable> cudf_table = std::make_unique<CudfTable>(in_table_view);
    std::vector<std::string> names(in_table_view.num_columns());
    std::unique_ptr<ral::frame::BlazingTable> table = std::make_unique<ral::frame::BlazingTable>(std::move(cudf_table), names);

    auto out_table = ral::processor::process_project(std::move(table),
                                                    "LogicalProject(EXPR$0=[SUBSTRING($0, 3, -1)])",
                                                    nullptr);

    cudf::test::strings_column_wrapper expected_col1{{"e","ick","own","x","mps","er","e","zy","g"}};
    cudf::table_view expected_table_view {{expected_col1}};

    cudf::test::expect_tables_equal(expected_table_view, out_table->view());
}

TEST_F(ProjectTestString, test_string_concat)
{
    cudf::test::strings_column_wrapper col1{{"foo", "d", "e", "a", "hello", "k", "d", "l", "", ""}};
    cudf::test::strings_column_wrapper col2{{"The", "quick", "brown", "fox", "jumps", "over", "the", "lazy", "dog", ""}};
  
    cudf::table_view in_table_view {{col1, col2}};
    std::unique_ptr<CudfTable> cudf_table = std::make_unique<CudfTable>(in_table_view);
    std::vector<std::string> names(in_table_view.num_columns());
    std::unique_ptr<ral::frame::BlazingTable> table = std::make_unique<ral::frame::BlazingTable>(std::move(cudf_table), names);

    auto out_table = ral::processor::process_project(std::move(table),
                                                    "LogicalProject(EXPR$0=[||($0,'XD=D')], EXPR$1=[||($0,$1)])",
                                                    nullptr);
  
    cudf::test::strings_column_wrapper expected_col1{{"fooXD=D","dXD=D","eXD=D","aXD=D","helloXD=D","kXD=D","dXD=D","lXD=D","XD=D","XD=D"}, {1, 1, 1, 1, 1, 1, 1, 1, 1, 1}};
    cudf::test::strings_column_wrapper expected_col2{{"fooThe","dquick","ebrown","afox","hellojumps","kover","dthe","llazy","dog",""}, {1, 1, 1, 1, 1, 1, 1, 1, 1, 1}};
    cudf::table_view expected_table_view {{expected_col1, expected_col2}};

    cudf::test::expect_tables_equal(expected_table_view, out_table->view());
}

TEST_F(ProjectTestString, test_cast_to_string)
{
    cudf::test::fixed_width_column_wrapper<bool> col1{{true, false, true, true, true, false, false, true, false, false}};
    cudf::test::fixed_width_column_wrapper<int32_t> col2{{1, 5, 10, 15, 100, 500, 1000, 5000, 10000, 999999}};
    cudf::test::fixed_width_column_wrapper<double> col3{{1.0, 5.5, 10.00003, 15.45, 100.656, 500.756756, 0.45435436, 0.0000324, 0.1, 999999.001}};
    cudf::test::fixed_width_column_wrapper<cudf::timestamp_s, cudf::timestamp_s::rep> col4{{0, 10, 2600, 89260, 579500, 6834000, 86796400, 135768000, 715360000, 1230720000}};
  
    cudf::table_view in_table_view {{col1, col2, col3, col4}};
    std::unique_ptr<CudfTable> cudf_table = std::make_unique<CudfTable>(in_table_view);
    std::vector<std::string> names(in_table_view.num_columns());
    std::unique_ptr<ral::frame::BlazingTable> table = std::make_unique<ral::frame::BlazingTable>(std::move(cudf_table), names);

    auto out_table = ral::processor::process_project(std::move(table),
                                                    "LogicalProject(EXPR$0=[CAST($0):VARCHAR], EXPR$1=[CAST($1):VARCHAR], EXPR$2=[CAST($2):VARCHAR], EXPR$3=[CAST($3):VARCHAR])",
                                                    nullptr);
  
    cudf::test::strings_column_wrapper expected_col1{{"true","false","true","true","true","false","false","true","false","false"}};
    cudf::test::strings_column_wrapper expected_col2{{"1","5","10","15","100","500","1000","5000","10000","999999"}};
    cudf::test::strings_column_wrapper expected_col3{{"1.0","5.5","10.00003","15.45","100.656","500.756756","0.45435436","3.24e-05","0.1","999999.001"}};
    cudf::test::strings_column_wrapper expected_col4{{"1970-01-01 00:00:00","1970-01-01 00:00:10","1970-01-01 00:43:20","1970-01-02 00:47:40","1970-01-07 16:58:20","1970-03-21 02:20:00","1972-10-01 14:06:40","1974-04-21 09:20:00","1992-09-01 15:06:40","2008-12-31 10:40:00"}};
    cudf::table_view expected_table_view {{expected_col1, expected_col2, expected_col3, expected_col4}};

    cudf::test::expect_tables_equal(expected_table_view, out_table->view());
}

TEST_F(ProjectTestString, test_cast_from_string)
{
    cudf::test::strings_column_wrapper col1{{"1","5","10","15","100","500","1000","5000","10000","999999"}};
    cudf::test::strings_column_wrapper col2{{"1.0","5.5","10.00003","15.45","100.656","500.756756","0.45435436","3.24e-05","0.1","999999.001"}};
    cudf::test::strings_column_wrapper col3{{"1970-01-01 00:00:00","1970-01-01 00:00:10","1970-01-01 00:43:20","1970-01-02 00:47:40","1970-01-07 16:58:20","1970-03-21 02:20:00","1972-10-01 14:06:40","1974-04-21 09:20:00","1992-09-01 15:06:40","2008-12-31 10:40:00"}};
  
    cudf::table_view in_table_view {{col1, col2, col3}};
    std::unique_ptr<CudfTable> cudf_table = std::make_unique<CudfTable>(in_table_view);
    std::vector<std::string> names(in_table_view.num_columns());
    std::unique_ptr<ral::frame::BlazingTable> table = std::make_unique<ral::frame::BlazingTable>(std::move(cudf_table), names);

    auto out_table = ral::processor::process_project(std::move(table),
                                                    "LogicalProject(EXPR$0=[CAST($0):INTEGER], EXPR$1=[CAST($1):DOUBLE], EXPR$2=[CAST($2):TIMESTAMP])",
                                                    nullptr);

    cudf::test::fixed_width_column_wrapper<int32_t> expected_col1{{1, 5, 10, 15, 100, 500, 1000, 5000, 10000, 999999}};
    cudf::test::fixed_width_column_wrapper<double> expected_col2{{1.0, 5.5, 10.00003, 15.45, 100.656, 500.756756, 0.45435436, 0.0000324, 0.1, 999999.001}};
    cudf::test::fixed_width_column_wrapper<cudf::timestamp_ns, cudf::timestamp_ns::rep> expected_col3{{
        0L,
        10000000000L,
        2600000000000L,
        89260000000000L,
        579500000000000L,
        6834000000000000L,
        86796400000000000L,
        135768000000000000L,
        715360000000000000L,
        1230720000000000000L }};
    cudf::table_view expected_table_view {{expected_col1, expected_col2, expected_col3}};

    cudf::test::expect_tables_equivalent(expected_table_view, out_table->view());
}

TEST_F(ProjectTestString, test_string_case)
{
    cudf::test::fixed_width_column_wrapper<int32_t> col1{{0, 1, 2, 3, 4, 5, 6, 7, 8}};
    cudf::test::strings_column_wrapper col2{{"The", "quick", "brown", "fox", "jumps", "over", "the", "lazy", "dog"}};
  
    cudf::table_view in_table_view {{col1, col2}};
    std::unique_ptr<CudfTable> cudf_table = std::make_unique<CudfTable>(in_table_view);
    std::vector<std::string> names(in_table_view.num_columns());
    std::unique_ptr<ral::frame::BlazingTable> table = std::make_unique<ral::frame::BlazingTable>(std::move(cudf_table), names);

    auto out_table = ral::processor::process_project(std::move(table),
                                                    "LogicalProject(EXPR$0=[CASE(=(MOD($0, 2), 0), $1, 'LOL')])",
                                                    nullptr);
  
    cudf::test::strings_column_wrapper expected_col1{{"The", "LOL", "brown", "LOL", "jumps", "LOL", "the", "LOL", "dog"}};
    cudf::table_view expected_table_view {{expected_col1}};

    cudf::test::expect_tables_equal(expected_table_view, out_table->view());
}

TEST_F(ProjectTestString, test_string_nested_case)
{
    cudf::test::fixed_width_column_wrapper<int32_t> col1{{0, 1, 2, 3, 4, 5, 6, 7, 8}};
    cudf::test::strings_column_wrapper col2{{"The", "quick", "brown", "fox", "jumps", "over", "the", "lazy", "dog"}};
  
    cudf::table_view in_table_view {{col1, col2}};
    std::unique_ptr<CudfTable> cudf_table = std::make_unique<CudfTable>(in_table_view);
    std::vector<std::string> names(in_table_view.num_columns());
    std::unique_ptr<ral::frame::BlazingTable> table = std::make_unique<ral::frame::BlazingTable>(std::move(cudf_table), names);

    auto out_table = ral::processor::process_project(std::move(table),
                                                    "LogicalProject(EXPR$0=[=(CASE(=(MOD($0, 2), 0), $1, 'LOL'), 'LOL')])",
                                                    nullptr);
  
    cudf::test::fixed_width_column_wrapper<bool> expected_col1{{false, true, false, true, false, true, false, true, false}, {1, 1, 1, 1, 1, 1, 1, 1, 1}};
    cudf::table_view expected_table_view {{expected_col1}};

    cudf::test::expect_tables_equal(expected_table_view, out_table->view());
}

template <typename T>
struct ProjectRoundTest : public BlazingUnitTest {};

TYPED_TEST_CASE(ProjectRoundTest, cudf::test::FloatingPointTypes);

TYPED_TEST(ProjectRoundTest, test_round)
{
    using T = TypeParam;

    cudf::test::fixed_width_column_wrapper<T> col1{{4.0, 5.21, 87317.3, 0.1232387, 0.0000007, 342.9348, 698.3243}};

    CudfTableView in_table_view{{col1}};
    std::unique_ptr<CudfTable> cudf_table = std::make_unique<CudfTable>(in_table_view);
    std::vector<std::string> names(in_table_view.num_columns());
    std::unique_ptr<ral::frame::BlazingTable> table = std::make_unique<ral::frame::BlazingTable>(std::move(cudf_table), names);

    auto out_table = ral::processor::process_project(std::move(table),
                                                    "LogicalProject(EXPR$0=[ROUND($0)], EXPR$1=[ROUND($0, 2)], EXPR$2=[ROUND($0, 5)])",
                                                    nullptr);

    cudf::test::fixed_width_column_wrapper<T> expect_col1{{4.0, 5.0, 87317.0, 0.0, 0.0, 343.0, 698.0}, {1, 1, 1, 1, 1, 1, 1}};
    cudf::test::fixed_width_column_wrapper<T> expect_col2{{4.00, 5.21, 87317.30, 0.12, 0.00, 342.93, 698.32}, {1, 1, 1, 1, 1, 1, 1}};
    cudf::test::fixed_width_column_wrapper<T> expect_col3{{4.00000, 5.21000, 87317.30000, 0.12324, 0.00000, 342.93480, 698.32430}, {1, 1, 1, 1, 1, 1, 1}};
    CudfTableView expect_cudf_table_view{{expect_col1, expect_col2, expect_col3}};

    cudf::test::expect_tables_equal(expect_cudf_table_view, out_table->view());
}
