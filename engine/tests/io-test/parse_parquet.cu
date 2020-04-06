#include <gmock/gmock.h>
#include <gtest/gtest.h>

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

#include <GDFColumn.cuh>

#include <GDFColumn.cuh>
#include <arrow/io/file.h>
#include <arrow/util/logging.h>

using blazingdb::manager::experimental::Context;
using Node = blazingdb::transport::experimental::Node;

#ifndef PARQUET_FILE_PATH
#error PARQUET_FILE_PATH must be defined for precompiling
#define PARQUET_FILE_PATH "/"
#endif


struct ParquetReaderAPITest : public ::testing::Test {
	void SetUp() { ASSERT_EQ(rmmInitialize(nullptr), RMM_SUCCESS); }

	void TearDown() { ASSERT_EQ(rmmFinalize(), RMM_SUCCESS); }
};

namespace cudf_io = cudf::experimental::io;

TEST_F(ParquetReaderAPITest, ByIdsInFromInterface) {
	std::string filename = PARQUET_FILE_PATH;

	std::vector<Uri> uris;
	uris.push_back(Uri{filename});
	ral::io::Schema schema;
	auto parser = std::make_shared<ral::io::parquet_parser>();
	auto provider = std::make_shared<ral::io::uri_data_provider>(uris);

	{
		ral::io::data_loader loader(parser, provider);
		try {
			loader.get_schema(schema, {});
			for(auto name : schema.get_names()) {
				std::cout << name << std::endl;
			}
			for(auto type : schema.get_types()) {
				std::cout << type << std::endl;
			}

		} catch(std::exception & e) {
			return;
		}
	}
	{
		cudf_io::read_parquet_args in_args{cudf_io::source_info{PARQUET_FILE_PATH}};
		auto result = cudf_io::read_parquet(in_args);
		for(auto name : result.metadata.column_names) {
			std::cout << "col_name: " << name << std::endl;
		}
		// expect_tables_equal(expected->view(), result.tbl->view());
	}
	Context queryContext{0, std::vector<std::shared_ptr<Node>>(), std::shared_ptr<Node>(), ""};
	ral::io::data_loader loader(parser, provider);

	auto csv_table = loader.load_data(&queryContext, {}, schema);
	if(csv_table != nullptr) {
		std::cout << "csv_table != nullptr\n";
		for(auto name : csv_table->names()) {
			std::cout << name << std::endl;
		}
		// expect_column_data_equal(std::vector<int32_t>{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
		// csv_table->view().column(0));
	}
	//     std::shared_ptr<::arrow::io::ReadableFile> file;
	//     const ::parquet::ReaderProperties          properties =
	//       ::parquet::default_reader_properties();
	//     ::arrow::io::ReadableFile::Open(filename, properties.memory_pool(), &file);


	//      std::vector<Uri> uris = {Uri{this->filename}};
	//     std::vector<bool> include_column = {true, false, true, false};
	//     std::unique_ptr<ral::io::data_provider> provider =
	// std::make_unique<ral::io::uri_data_provider>(uris);
	//     std::unique_ptr<ral::io::data_parser> parser =
	// std::make_unique<ral::io::parquet_parser>();


	//     EXPECT_TRUE(provider->has_next());
	//     std::vector<gdf_column_cpp> gdf_columns_cpp;
	//     parser->parse(provider->get_next(), gdf_columns_cpp, include_column);

	//     for(size_t column_index = 0; column_index < gdf_columns_cpp.size();
	// column_index++){ std::cout << "col_name: " <<
	// gdf_columns_cpp[column_index].get_gdf_column()->col_name << std::endl;
	//         print_gdf_column(gdf_columns_cpp[column_index].get_gdf_column());
	//     }
}