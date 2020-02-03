//
// Created by aocsa on 12/9/19.
//

#ifndef BLAZINGDB_RAL_SRC_IO_DATA_PARSER_METADATA_PARQUET_METADATA_H_
#define BLAZINGDB_RAL_SRC_IO_DATA_PARSER_METADATA_PARQUET_METADATA_H_

#include <vector>
#include <memory>
#include <cudf/cudf.h>

#include <parquet/api/reader.h>
#include <execution_graph/logic_controllers/LogicPrimitives.h>


std::unique_ptr<ral::frame::BlazingTable> get_minmax_metadata(
	std::vector<std::unique_ptr<parquet::ParquetFileReader>> &parquet_readers,
	size_t total_num_row_groups, int metadata_offset);

#endif	// BLAZINGDB_RAL_SRC_IO_DATA_PARSER_METADATA_PARQUET_METADATA_H_
