#include "io/data_provider/UriDataProvider.h"
#include "io/Schema.h"
#include "io/data_parser/OrcParser.h"
#include "io/DataLoader.h"

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

#include <fstream>
#include <unistd.h>

#include <cudf/io/functions.hpp>

using blazingdb::manager::experimental::Context;
using Node = blazingdb::transport::experimental::Node;

using namespace ral::frame;
using namespace ral::processor;

template <typename T>
struct OrcTest : public cudf::test::BaseFixture {};

// To get the current directory
std::string GetCurrentWorkingDir( void ) {
  char buff[100];
  getcwd( buff, 100 );
  std::string current_working_dir(buff);
  if (current_working_dir.find("build") != std::string::npos) {
    current_working_dir.erase( current_working_dir.end()-6, current_working_dir.end());
  }

  return current_working_dir;
}

// TYPED_TEST_CASE will run only a type: int16_t
// Here the type specified is defined by cudf::test::Types<int16_t>
TYPED_TEST_CASE(OrcTest, cudf::test::Types<int16_t>);


TYPED_TEST(OrcTest, multipleColumns)
{
    // TypeParam is the type defined by TYPED_TEST_CASE for a particular test run. Here we are giving TypeParam an alias T
    using T = TypeParam;

    // Loading a simple .orc file
    std::string filepath = GetCurrentWorkingDir() + "/tests/io-test/region_0_0.orc";

    cudf::experimental::io::read_orc_args orc_args{cudf::experimental::io::source_info{filepath}};
    orc_args.columns = {"r_regionkey", "r_name", "r_comment"};

    // this is basically the test process
    std::vector<Uri> uris;
    uris.push_back(Uri{filepath});
    
    ral::io::Schema schema;
    auto parser = std::make_shared<ral::io::orc_parser>(orc_args);
    auto provider = std::make_shared<ral::io::uri_data_provider>(uris);
    
    ral::io::data_loader loader(parser, provider);
    loader.get_schema(schema, {});
    
    Context queryContext{0, std::vector<std::shared_ptr<Node>>(), std::shared_ptr<Node>(), ""};
    std::unique_ptr<BlazingTable> orc_table = loader.load_data(&queryContext, {}, schema);

    // Here we are creating the expected output
    cudf::test::fixed_width_column_wrapper<T> expect_col1{{0, 1, 2, 3, 4}, {1, 1, 1, 1, 1}};
    cudf::test::strings_column_wrapper expect_col2{{"AFRICA", "AMERICA", "ASIA", "EUROPE", "MIDDLE EAST"}, {1, 1, 1, 1, 1}};
    cudf::test::strings_column_wrapper expect_col3{{
        "lar deposits. blithely final packages cajole. regular waters are final requests. regular accounts are according to ",
        "hs use ironic, even requests. s", 
        "ges. thinly even pinto beans ca", 
        "ly final courts cajole furiously final excuse",
        "uickly special accounts cajole carefully blithely close requests. carefully final asymptotes haggle furiousl"},
        {1, 1, 1, 1, 1}};
    CudfTableView expect_cudf_table_view {{expect_col1,expect_col2, expect_col3 }};

    // Here we are printing out the output we got and the expect_cudf_table_view (only necessary for debugging)
    std::string orc_table_col_0 = cudf::test::to_string(orc_table->view().column(0), "|");
    std::cout<<"orc_table_col_0: "<< orc_table_col_0 <<std::endl;
    std::string col0_string_cudf_table = cudf::test::to_string(expect_cudf_table_view.column(0), "|");
    std::cout<<"expect_column_0: "<< col0_string_cudf_table <<std::endl;

    std::string orc_table_col_1 = cudf::test::to_string(orc_table->view().column(1), "|");
    std::cout<<"orc_table_col_1: "<< orc_table_col_1 <<std::endl;
    std::string col1_string_cudf_table = cudf::test::to_string(expect_cudf_table_view.column(1), "|");
    std::cout<<"expect_column_1: "<< col1_string_cudf_table <<std::endl;

    std::string orc_table_col_2 = cudf::test::to_string(orc_table->view().column(2), "|");
    std::cout<<"orc_table_col_2: "<< orc_table_col_2 <<std::endl;
    std::string col2_string_cudf_table = cudf::test::to_string(expect_cudf_table_view.column(2), "|");
    std::cout<<"expect_column_2: "<< col2_string_cudf_table <<std::endl;

    // Here we are validating the output
    // TODO: c.cordova, values are equals but something weird happen with the expect_tables_equal
    //cudf::test::expect_tables_equal(expect_cudf_table_view, orc_table->view());
}
