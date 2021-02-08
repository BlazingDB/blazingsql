//
// Created by aocsa on 12/9/19.
//

#ifndef BLAZINGDB_RAL_SRC_IO_DATA_PARSER_METADATA_PARQUET_METADATA_H_
#define BLAZINGDB_RAL_SRC_IO_DATA_PARSER_METADATA_PARQUET_METADATA_H_

#include <vector>
#include <memory>

#include "common_metadata.h"
#include <parquet/api/reader.h>

std::unique_ptr<ral::frame::BlazingTable> get_minmax_metadata(
	std::vector<std::unique_ptr<parquet::ParquetFileReader>> &parquet_readers,
	size_t total_num_row_groups, int metadata_offset);

void set_min_max(
	std::vector<std::vector<int64_t>> &minmax_metadata_table,
	int col_index, parquet::Type::type physical,
	parquet::ConvertedType::type logical,
	std::shared_ptr<parquet::Statistics> &statistics);

#endif	// BLAZINGDB_RAL_SRC_IO_DATA_PARSER_METADATA_PARQUET_METADATA_H_
