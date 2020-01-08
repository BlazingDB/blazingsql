#include <gtest/gtest.h>
#include <gmock/gmock.h>

#include <cstdlib>
#include <iostream>
#include <string>
#include <vector>

#include "Traits/RuntimeTraits.h"

#include "io/DataLoader.h"
#include "io/data_parser/CSVParser.h"
#include "io/data_parser/DataParser.h"
#include "io/data_parser/ParquetParser.h"
#include "io/data_provider/DataProvider.h"
#include "io/data_provider/UriDataProvider.h"
#include <DataFrame.h>
#include <fstream>
#include <gdf_wrapper/gdf_wrapper.cuh>

#include <GDFColumn.cuh>

#include <GDFColumn.cuh>
#include <arrow/io/file.h>
#include <arrow/util/logging.h>

using blazingdb::manager::Context;
using Node = blazingdb::transport::Node;

struct ParseCSVTest : public ::testing::Test {

  void SetUp() { ASSERT_EQ(rmmInitialize(nullptr), RMM_SUCCESS); }

  void TearDown() { ASSERT_EQ(rmmFinalize(), RMM_SUCCESS); }
};

namespace cudf_io = cudf::experimental::io;


  // Helper function to compare two floating-point column contents
template <typename T>
void expect_column_data_equal(std::vector<T> const& lhs,
                              cudf::column_view const& rhs) {
  EXPECT_THAT(cudf::test::to_host<T>(rhs).first, lhs);
}
 

const std::string content =
R"(0|ALGERIA|0| haggle. carefully final deposits detect slyly agai
1|ARGENTINA|1|al foxes promise slyly according to the regular accounts. bold requests alon
2|BRAZIL|1|y alongside of the pending deposits. carefully special packages are about the ironic forges. slyly special 
3|CANADA|1|eas hang ironic, silent packages. slyly regular packages are furiously over the tithes. fluffily bold
4|EGYPT|4|y above the carefully unusual theodolites. final dugouts are quickly across the furiously regular d
5|ETHIOPIA|0|ven packages wake quickly. regu
6|FRANCE|3|refully final requests. regular, ironi
7|GERMANY|3|l platelets. regular accounts x-ray: unusual, regular acco
8|INDIA|2|ss excuses cajole slyly across the packages. deposits print aroun
9|INDONESIA|2| slyly express asymptotes. regular deposits haggle slyly. carefully ironic hockey players sleep blithely. carefull
10|IRAN|4|efully alongside of the slyly final dependencies)";

TEST_F(ParseCSVTest, csv_with_strings) {
  std::cout << "csv_with_strings\n";
  std::string filename = "/tmp/nation.psv";
  std::ofstream outfile(filename, std::ofstream::out);
  outfile << content << std::endl;
  outfile.close();
  std::vector<std::string> names{"n_nationkey", "n_name", "n_regionkey", "n_comment"};
  
  std::vector<std::string> files = {filename}; 

  cudf_io::read_csv_args in_args{cudf_io::source_info{filename}};
  in_args.names = {"n_nationkey", "n_name", "n_regionkey", "n_comment"};
  in_args.dtype = { "int32", "int64", "int32", "int64"};
  in_args.delimiter = '|';
  in_args.header = -1;
  

  std::vector<Uri> uris;

  uris.push_back(Uri{filename});
  ral::io::Schema schema;
  auto parser = std::make_shared<ral::io::csv_parser>(in_args);
  auto provider = std::make_shared<ral::io::uri_data_provider>(uris);

  { 
    ral::io::data_loader loader(parser, provider);
    try {
      loader.get_schema(schema, {});
      for (auto name : schema.get_names()) {
        std::cout << name << std::endl;
      }
      for (auto type : schema.get_types()) {
        std::cout << type << std::endl;
      }

    } catch (std::exception &e) {
      return;
    }
  }

  Context queryContext{0, std::vector<std::shared_ptr<Node>>(), std::shared_ptr<Node>(), ""};
  ral::io::data_loader loader(parser, provider);

  auto csv_table = loader.load_data(queryContext, {}, schema);
  if (csv_table != nullptr) {
    std::cout << "csv_table != nullptr\n";
    for (auto name : csv_table->names()) {
        std::cout << name << std::endl;
    }
    expect_column_data_equal(std::vector<int32_t>{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10}, csv_table->view().column(0));
  }

  auto result = cudf_io::read_csv(in_args);
  const auto view = result.tbl->view();
  
  EXPECT_EQ(4, view.num_columns());
  ASSERT_EQ(cudf::type_id::INT32, view.column(0).type().id());
  ASSERT_EQ(cudf::type_id::INT64, view.column(1).type().id());
  ASSERT_EQ(cudf::type_id::INT32, view.column(2).type().id());
  ASSERT_EQ(cudf::type_id::INT64, view.column(3).type().id());

  expect_column_data_equal(std::vector<int32_t>{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10}, view.column(0));


//   for (size_t column_index = 0; column_index < input_table.size();
//        column_index++) {
//     std::cout << "col_name: "
//               << input_table[column_index].get_gdf_column()->col_name << "|"
//               << input_table[column_index].get_gdf_column()->size << std::endl;
//     print_gdf_column(input_table[column_index].get_gdf_column());
//   }
}