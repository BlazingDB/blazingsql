#include <gtest/gtest.h>

#include <cstdlib>
#include <iostream>
#include <string>
#include <vector>


#include "io/DataLoader.h"
#include "io/data_parser/DataParser.h"
#include "io/data_parser/ParquetParser.h"
#include "io/data_provider/DataProvider.h"
#include "io/data_provider/UriDataProvider.h"
#include <fstream>


#include <arrow/io/file.h>
#include <arrow/util/logging.h>

#include "utilities/DebuggingUtils.h"
 

#ifndef PARQUET_FILE_PATH
#error PARQUET_FILE_PATH must be defined for precompiling
#define PARQUET_FILE_PATH "/"
#endif

using blazingdb::manager::experimental::Context;
using Node = blazingdb::transport::experimental::Node;


struct MinMaxParquetTest : public ::testing::Test {

  void SetUp() { ASSERT_EQ(rmmInitialize(nullptr), RMM_SUCCESS); }

  void TearDown() { ASSERT_EQ(rmmFinalize(), RMM_SUCCESS); }
};


TEST_F(MinMaxParquetTest, UsingRalIO) {

  std::string filename = PARQUET_FILE_PATH;
	std::cout << "filename: " << filename << std::endl;
	
	std::vector<Uri> uris;
	uris.push_back(Uri{filename});
	auto parser = std::make_shared<ral::io::parquet_parser>();
	auto provider = std::make_shared<ral::io::uri_data_provider>(uris);
 
	auto loader = std::make_shared<ral::io::data_loader>(parser, provider);
	std::cout << ">>> reading metadata:: loaded " <<   std::endl;
	try {
		std::unique_ptr<ral::frame::BlazingTable> metadata = loader->get_metadata(0);
		std::cout << ">>> reading metadata:: got metadata " <<   std::endl;
		auto view = metadata->toBlazingTableView();
		ral::utilities::print_blazing_table_view(view);
	} catch(std::exception e){
		std::cerr << "***std::exception:: " <<  e.what() << std::endl;

	}
}
