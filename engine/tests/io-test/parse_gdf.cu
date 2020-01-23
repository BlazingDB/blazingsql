#include "io/Schema.h"
#include "io/data_parser/GDFParser.h" 
#include "io/DataLoader.h"
#include "io/data_provider/DummyProvider.h"

#include <cudf/cudf.h>
#include <cudf/types.hpp>
#include <from_cudf/cpp_tests/utilities/base_fixture.hpp>
#include <cudf/table/table_view.hpp>
#include <from_cudf/cpp_tests/utilities/column_utilities.hpp>
#include <cudf/utilities/type_dispatcher.hpp>
#include <from_cudf/cpp_tests/utilities/type_lists.hpp>
#include <from_cudf/cpp_tests/utilities/column_wrapper.hpp>
#include <from_cudf/cpp_tests/utilities/table_utilities.hpp>
#include <vector>
#include <execution_graph/logic_controllers/LogicalFilter.h>
#include <execution_graph/logic_controllers/LogicPrimitives.h>

#include <cudf/io/functions.hpp>


using namespace ral::frame;
using namespace ral::processor;

using blazingdb::manager::experimental::Context;
using Node = blazingdb::transport::experimental::Node;


template <typename T>
struct GDFTest : public cudf::test::BaseFixture {};

// TYPED_TEST_CASE will run only a type: int16_t
TYPED_TEST_CASE(GDFTest, cudf::test::NumericTypes);


TYPED_TEST(GDFTest, multipleColumns)
{
    // TypeParam is the type defined by TYPED_TEST_CASE for a particular test run. Here we are giving TypeParam an alias T
    using T = TypeParam;

    // Here we are creating the input data
    cudf::test::fixed_width_column_wrapper<T> col1{{5, 4, 3, 5, 8, 5, 6}, {1, 1, 1, 1, 1, 1, 1}};
    cudf::test::strings_column_wrapper col2({"d", "e", "a", "d", "k", "d", "l"}, {1, 1, 1, 1, 1, 1, 1});
    cudf::test::fixed_width_column_wrapper<T> col3{{10, 40, 70, 5, 2, 10, 11}, {1, 1, 1, 1, 1, 1, 1}};

    // Only this variables are necessary
    std::vector<std::string> names({"A", "B", "C"});
    std::vector<cudf::column *> columns; 
    columns.emplace_back(col1.release().release()); 
    columns.emplace_back(col2.release().release());
    columns.emplace_back(col3.release().release());

    // this is basically the test process
    auto parser = std::make_shared<ral::io::gdf_parser>(columns, names);
    auto provider = std::make_shared<ral::io::dummy_data_provider>();
    ral::io::data_loader loader(parser, provider);
    ral::io::Schema schema(names, {} );
    
    Context queryContext{0, std::vector<std::shared_ptr<Node>>(), std::shared_ptr<Node>(), ""}; 
    std::unique_ptr<BlazingTable> table_out = loader.load_data(&queryContext, {}, schema); 

    // Here we are creating the expected output (same as the begin)
    cudf::test::fixed_width_column_wrapper<T> expect_col1{{5, 4, 3, 5, 8, 5, 6}, {1, 1, 1, 1, 1, 1, 1}};
    cudf::test::strings_column_wrapper expect_col2({"d", "e", "a", "d", "k", "d", "l"}, {1, 1, 1, 1, 1, 1, 1});
    cudf::test::fixed_width_column_wrapper<T> expect_col3{{10, 40, 70, 5, 2, 10, 11}, {1, 1, 1, 1, 1, 1, 1}};
    CudfTableView expect_cudf_table_view {{expect_col1, expect_col2, expect_col3}};

    // Here we are printing out the output we got (this is optional and only necessary for debugging)
    std::string col0_string = cudf::test::to_string(table_out->view().column(0), "|");
    std::cout << "col0_string: " << col0_string << std::endl;
    std::string col1_string = cudf::test::to_string(table_out->view().column(1), "|");
    std::cout <<"col1_string: " << col1_string << std::endl;
    std::string col2_string = cudf::test::to_string(table_out->view().column(2), "|");
    std::cout << "col2_string: " << col2_string << std::endl;

    // Here we are validating the output
    cudf::test::expect_tables_equal(expect_cudf_table_view, table_out->view());
}
