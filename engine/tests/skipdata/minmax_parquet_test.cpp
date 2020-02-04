#include <gtest/gtest.h>

#include <cstdlib>
#include <iostream>
#include <string>
#include <vector>


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

#include "execution_graph/logic_controllers/LogicalFilter.h"
#include "utilities/DebuggingUtils.h"
 

#ifndef PARQUET_FILE_PATH
#error PARQUET_FILE_PATH must be defined for precompiling
#define PARQUET_FILE_PATH "/"
#endif

TEST(MinMaxParquetTest, UsingRalIO) {
  std::vector<Uri> uris;
  uris.push_back(Uri{PARQUET_FILE_PATH});
  std::cout << "PARQUET_FILE_PATH: " << PARQUET_FILE_PATH << std::endl;
  
  auto parser = std::make_shared<ral::io::parquet_parser>();
  auto provider = std::make_shared<ral::io::uri_data_provider>(uris);

  auto loader = std::make_shared<ral::io::data_loader>(parser, provider);
 
  ral::io::Schema schema;
  loader->get_schema(schema, {});

//  std::cout << "schema: " << schema.get_num_columns() << std::endl;

//  auto nfiles = 1;
//  std::unique_ptr<ral::frame::BlazingTable> input_table = loader.get_metadata(0);

//  std::cout << "metadata: " << input_table->num_columns() << std::endl;

//  ral::utilities::print_blazing_table_view(input_table->toBlazingTableView());
}
