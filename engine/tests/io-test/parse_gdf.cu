#include "io/Schema.h"
#include "io/data_parser/GDFParser.h"   // ERROR when include that header
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

using blazingdb::manager::Context;
using Node = blazingdb::transport::Node;


template <typename T>
struct GDFTest : public cudf::test::BaseFixture {};

// TYPED_TEST_CASE will run only a type: int16_t
// Here the type specified is defined by cudf::test::Types<int16_t>
TYPED_TEST_CASE(GDFTest, cudf::test::Types<int16_t>);


TYPED_TEST(GDFTest, multipleColumns)
{
    // TypeParam is the type defined by TYPED_TEST_CASE for a particular test run. Here we are giving TypeParam an alias T
    using T = TypeParam;

    // GDF data should be created here 


    // this is basically the test process
    
    // CREAR UN TABLESCHEMA y pasarle data MANUALMENTE
    //TableSchema tableSchema;
    //auto parser = std::make_shared<ral::io::gdf_parser>(tableSchema);

    auto provider = std::make_shared<ral::io::dummy_data_provider>();
    
    //ral::io::data_loader loader(parser, provider);

    ral::io::Schema schema;
    //loader.get_schema(schema, {});
    
    Context queryContext{0, std::vector<std::shared_ptr<Node>>(), std::shared_ptr<Node>(), ""};
    //std::unique_ptr<BlazingTable> gdf_table = loader.load_data(&queryContext, {}, schema);

    
    /*
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
    */
}
