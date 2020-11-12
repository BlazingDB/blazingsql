//
// Created by aocsa on 12/9/19.
//

#ifndef BLAZINGDB_RAL_SRC_IO_DATA_PARSER_METADATA_PARQUET_METADATA_H_
#define BLAZINGDB_RAL_SRC_IO_DATA_PARSER_METADATA_PARQUET_METADATA_H_

#include <vector>
#include <memory>
//#include <cudf/cudf.h>

#include <parquet/api/reader.h>
#include <execution_graph/logic_controllers/LogicPrimitives.h>


std::unique_ptr<ral::frame::BlazingTable> get_minmax_metadata(
	std::vector<std::unique_ptr<parquet::ParquetFileReader>> &parquet_readers,
	size_t total_num_row_groups, int metadata_offset);

std::basic_string<char> get_typed_vector_content(
	cudf::type_id dtype, std::vector<int64_t> &vector);

std::unique_ptr<cudf::column> make_cudf_column_from(
	cudf::data_type dtype, std::basic_string<char> &vector, unsigned long column_size);

void set_min_max(
	std::vector<std::vector<int64_t>> &minmax_metadata_table,
	int col_index, parquet::Type::type physical,
	parquet::ConvertedType::type logical,
	std::shared_ptr<parquet::Statistics> &statistics);

#endif	// BLAZINGDB_RAL_SRC_IO_DATA_PARSER_METADATA_PARQUET_METADATA_H_
