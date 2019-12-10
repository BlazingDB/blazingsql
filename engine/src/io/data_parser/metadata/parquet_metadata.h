//
// Created by aocsa on 12/9/19.
//

#ifndef BLAZINGDB_RAL_SRC_IO_DATA_PARSER_METADATA_PARQUET_METADATA_H_
#define BLAZINGDB_RAL_SRC_IO_DATA_PARSER_METADATA_PARQUET_METADATA_H_

#include <algorithm>
#include <string>
#include <unordered_map>
#include <vector>
#include <memory>
#include <algorithm>
#include <numeric>
#include <string>
#include <unordered_map>
#include <vector>
#include <arrow/io/file.h>
#include <parquet/file_reader.h>
#include <parquet/schema.h>
#include <parquet/types.h>
#include <thread>

#include <parquet/column_writer.h>
#include <parquet/file_writer.h>
#include <parquet/properties.h>
#include <parquet/schema.h>
#include <parquet/types.h>

#include <parquet/api/reader.h>
#include <parquet/api/writer.h>

std::vector<gdf_column*> get_minmax_metadata(
	std::vector<std::unique_ptr<parquet::ParquetFileReader>> &parquet_readers,
	const std::vector<std::string> &user_readable_file_handles,
	size_t total_num_row_groups);

#endif	// BLAZINGDB_RAL_SRC_IO_DATA_PARSER_METADATA_PARQUET_METADATA_H_
