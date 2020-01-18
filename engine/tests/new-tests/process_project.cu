#include "from_cudf/cpp_tests/utilities/base_fixture.hpp"
#include "from_cudf/cpp_tests/utilities/column_utilities.hpp"
#include "from_cudf/cpp_tests/utilities/column_wrapper.hpp"
#include "from_cudf/cpp_tests/utilities/table_utilities.hpp"
#include "from_cudf/cpp_tests/utilities/type_lists.hpp"

#include "execution_graph/logic_controllers/LogicalProject.h"

#include <execution_graph/logic_controllers/LogicPrimitives.h>

template <typename T>
struct ProjectTestNumeric : public cudf::test::BaseFixture {};

TYPED_TEST_CASE(ProjectTestNumeric, cudf::test::NumericTypes);

TYPED_TEST(ProjectTestNumeric, test_numeric_types1)
{
    using T = TypeParam;

    cudf::test::fixed_width_column_wrapper<T> col1{{4, 5, 3, 5, 8, 5, 6}, {1, 1, 1, 1, 1, 1, 1}};
    cudf::test::strings_column_wrapper col2({"b", "d", "a", "d", "l", "d", "k"}, {1, 1, 1, 1, 1, 1, 1});
    cudf::test::fixed_width_column_wrapper<T> col3{{10, 40, 70, 5, 2, 10, 11}, {1, 1, 1, 1, 1, 1, 1}};

    CudfTableView cudf_table_in_view {{col1, col2, col3}};

    std::vector<std::string> names({"A", "B", "C"});
    ral::frame::BlazingTableView table(cudf_table_in_view, names);

    std::string query_part = "LogicalProject(A=[$0])";
    blazingdb::manager::Context * context;

    std::unique_ptr<ral::frame::BlazingTable> table_out = ral::processor::process_project(
        table, 
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

    std::vector<std::string> names({"A", "B", "C"});
    ral::frame::BlazingTableView table(cudf_table_in_view, names);

    std::string query_part = "LogicalProject(C=[$2])";
    blazingdb::manager::Context * context;

    std::unique_ptr<ral::frame::BlazingTable> table_out = ral::processor::process_project(
        table, 
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

    std::vector<std::string> names({"A", "B", "C"});
    ral::frame::BlazingTableView table(cudf_table_in_view, names);

    std::string query_part = "LogicalProject(A=[$0], C=[$2])";
    blazingdb::manager::Context * context;

    std::unique_ptr<ral::frame::BlazingTable> table_out = ral::processor::process_project(
        table, 
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

    std::vector<std::string> names({"A", "B", "C"});
    ral::frame::BlazingTableView table(cudf_table_in_view, names);

    std::string query_part = "LogicalProject(EXPR$0=[1])";
    blazingdb::manager::Context * context;

    std::unique_ptr<ral::frame::BlazingTable> table_out = ral::processor::process_project(
        table, 
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

    std::vector<std::string> names({"A", "B", "C"});
    ral::frame::BlazingTableView table(cudf_table_in_view, names);

    std::string query_part = "LogicalProject(EXPR$0=[>($0, 3)])";
    blazingdb::manager::Context * context;

    std::unique_ptr<ral::frame::BlazingTable> table_out = ral::processor::process_project(
        table, 
        query_part,
        context);

    if (std::is_same<T, cudf::experimental::bool8>::value) {
        cudf::test::fixed_width_column_wrapper<cudf::experimental::bool8> expect_col1{{0, 0, 0, 0, 0, 0, 0}};
        CudfTableView expect_cudf_table_view {{expect_col1}};

        cudf::test::expect_tables_equal(expect_cudf_table_view, table_out->view());
    } else {
        cudf::test::fixed_width_column_wrapper<cudf::experimental::bool8> expect_col1{{1, 1, 0, 1, 1, 1, 1}};
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

    std::vector<std::string> names({"A", "B", "C"});
    ral::frame::BlazingTableView table(cudf_table_in_view, names);

    std::string query_part = "LogicalProject(EXPR$0=[>($0, 3)], EXPR$1=[<($2, 3)])";
    blazingdb::manager::Context * context;

    std::unique_ptr<ral::frame::BlazingTable> table_out = ral::processor::process_project(
        table, 
        query_part,
        context);

    if (std::is_same<T, cudf::experimental::bool8>::value) {
        cudf::test::fixed_width_column_wrapper<cudf::experimental::bool8> expect_col1{{0, 0, 0, 0, 0, 0, 0}};
        cudf::test::fixed_width_column_wrapper<cudf::experimental::bool8> expect_col3{{1, 1, 1, 1, 1, 1, 1}};
        CudfTableView expect_cudf_table_view {{expect_col1, expect_col3}};

        std::string col0_string = cudf::test::to_string(table_out->view().column(0), "|");
        std::cout<<"col0_string: "<<col0_string<<std::endl;

        std::string col2_string = cudf::test::to_string(table_out->view().column(1), "|");
        std::cout<<"col2_string: "<<col2_string<<std::endl;

        cudf::test::expect_tables_equal(expect_cudf_table_view, table_out->view());
    } else {
        cudf::test::fixed_width_column_wrapper<cudf::experimental::bool8> expect_col1{{1, 1, 0, 1, 1, 1, 1}};
        cudf::test::fixed_width_column_wrapper<cudf::experimental::bool8> expect_col3{{0, 0, 0, 0, 1, 0, 0}};
        CudfTableView expect_cudf_table_view {{expect_col1, expect_col3}};
    
        std::string col0_string = cudf::test::to_string(table_out->view().column(0), "|");
        std::cout<<"col0_string: "<<col0_string<<std::endl;

        std::string col2_string = cudf::test::to_string(table_out->view().column(1), "|");
        std::cout<<"col2_string: "<<col2_string<<std::endl;

        cudf::test::expect_tables_equal(expect_cudf_table_view, table_out->view());
    }
}