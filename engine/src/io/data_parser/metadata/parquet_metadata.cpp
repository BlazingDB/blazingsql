//
// Created by aocsa on 12/9/19.
//

#ifndef BLAZINGDB_RAL_SRC_IO_DATA_PARSER_METADATA_PARQUET_METADATA_CPP_H_
#define BLAZINGDB_RAL_SRC_IO_DATA_PARSER_METADATA_PARQUET_METADATA_CPP_H_

#include "parquet_metadata.h"
#include "parquet/api/reader.h"
#include "parquet/statistics.h"
#include <iostream>
#include <mutex>
#include <rmm/rmm.h>
#include <src/DataFrame.h>
#include <src/Traits/RuntimeTraits.h>
#include <src/gdf_wrapper/utilities/error_utils.h>
#include <thread>
//#include "GDFColumn.cuh"

void set_min_max(
	std::vector<std::vector<int64_t>> &minmax_metadata_table,
	int col_index, parquet::Type::type physical,
	parquet::ConvertedType::type logical,
	std::shared_ptr<parquet::Statistics> &statistics) {

	switch (logical) {
	case parquet::ConvertedType::type::UINT_8:
	case parquet::ConvertedType::type::INT_8:
	case parquet::ConvertedType::type::UINT_16:
	case parquet::ConvertedType::type::INT_16:
	case parquet::ConvertedType::type::DATE:
		physical = parquet::Type::type::INT32;
		break;
	case parquet::ConvertedType::type::TIMESTAMP_MILLIS: {
		auto convertedStats =
			std::static_pointer_cast<parquet::Int64Statistics>(statistics);
		// TODO, review this case
		// gdf_dtype_extra_info{TIME_UNIT_ms}
		auto min = convertedStats->min();
		auto max = convertedStats->max();
		minmax_metadata_table[col_index].push_back(min);
		minmax_metadata_table[col_index + 1].push_back(max);
		break;
	}
	case parquet::ConvertedType::type::TIMESTAMP_MICROS: {
		auto convertedStats =
			std::static_pointer_cast<parquet::Int64Statistics>(statistics);
		// gdf_dtype_extra_info{TIME_UNIT_us}
		// TODO, review this case
		auto min = convertedStats->min();
		auto max = convertedStats->max();
		minmax_metadata_table[col_index].push_back(min);
		minmax_metadata_table[col_index + 1].push_back(max);
		break;
	}
	default:
		break;
	}

	// Physical storage type supported by Parquet; controls the on-disk storage
	// format in combination with the encoding type.
	switch (physical) {
	case parquet::Type::type::BOOLEAN: {
		auto convertedStats =
			std::static_pointer_cast<parquet::BoolStatistics>(statistics);
		auto min = convertedStats->min();
		auto max = convertedStats->max();
		minmax_metadata_table[col_index].push_back(min);
		minmax_metadata_table[col_index + 1].push_back(max);
		break;
	}
	case parquet::Type::type::INT32: {
		auto convertedStats =
			std::static_pointer_cast<parquet::Int32Statistics>(statistics);
		auto min = convertedStats->min();
		auto max = convertedStats->max();
		minmax_metadata_table[col_index].push_back(min);
		minmax_metadata_table[col_index + 1].push_back(max);

		break;
	}
	case parquet::Type::type::INT64: {
		auto convertedStats =
			std::static_pointer_cast<parquet::Int64Statistics>(statistics);
		auto min = convertedStats->min();
		auto max = convertedStats->max();
		minmax_metadata_table[col_index].push_back(min);
		minmax_metadata_table[col_index + 1].push_back(max);
		break;
	}
	case parquet::Type::type::FLOAT: {
		auto convertedStats =
			std::static_pointer_cast<parquet::FloatStatistics>(statistics);
		auto min = convertedStats->min();
		auto max = convertedStats->max();
		minmax_metadata_table[col_index].push_back(min);
		minmax_metadata_table[col_index + 1].push_back(max);
		break;
	}
	case parquet::Type::type::DOUBLE: {
		auto convertedStats =
			std::static_pointer_cast<parquet::DoubleStatistics>(statistics);
		auto min = convertedStats->min();
		auto max = convertedStats->max();
		minmax_metadata_table[col_index].push_back(min);
		minmax_metadata_table[col_index + 1].push_back(max);
		break;
	}
	case parquet::Type::type::BYTE_ARRAY:
	case parquet::Type::type::FIXED_LEN_BYTE_ARRAY: {
		auto convertedStats =
			std::static_pointer_cast<parquet::FLBAStatistics>(statistics);
		// No min max for String columns
		// minmax_metadata_table[col_index].push_back(-1);
		// minmax_metadata_table[col_index + 1].push_back(-1);
		break;
	}
	case parquet::Type::type::INT96: {
		// "Dont know how to handle INT96 min max"
		// Convert Spark INT96 timestamp to GDF_DATE64
		// return std::make_tuple(GDF_DATE64, 0, 0);
	}
	default:
		throw std::runtime_error("Invalid gdf_dtype in set_min_max");
		break;
	}
}



// This function is copied and adapted from cudf
std::pair<gdf_dtype, gdf_dtype_extra_info>
to_dtype(parquet::Type::type physical, parquet::ConvertedType::type logical) {

	bool strings_to_categorical = false; // parameter used in cudf::read_parquet

	// Logical type used for actual data interpretation; the legacy converted type
	// is superceded by 'logical' type whenever available.
	switch (logical) {
	case parquet::ConvertedType::type::UINT_8:
	case parquet::ConvertedType::type::INT_8:
		return std::make_pair(GDF_INT8, gdf_dtype_extra_info{TIME_UNIT_NONE});
	case parquet::ConvertedType::type::UINT_16:
	case parquet::ConvertedType::type::INT_16:
		return std::make_pair(GDF_INT16, gdf_dtype_extra_info{TIME_UNIT_NONE});
	case parquet::ConvertedType::type::DATE:
		return std::make_pair(GDF_DATE32, gdf_dtype_extra_info{TIME_UNIT_NONE});
	case parquet::ConvertedType::type::TIMESTAMP_MILLIS:
		return std::make_pair(GDF_DATE64, gdf_dtype_extra_info{TIME_UNIT_ms});
	case parquet::ConvertedType::type::TIMESTAMP_MICROS:
		return std::make_pair(GDF_DATE64, gdf_dtype_extra_info{TIME_UNIT_us});
	default:
		break;
	}

	// Physical storage type supported by Parquet; controls the on-disk storage
	// format in combination with the encoding type.
	switch (physical) {
	case parquet::Type::type::BOOLEAN:
		return std::make_pair(GDF_BOOL8, gdf_dtype_extra_info{TIME_UNIT_NONE});
	case parquet::Type::type::INT32:
		return std::make_pair(GDF_INT32, gdf_dtype_extra_info{TIME_UNIT_NONE});
	case parquet::Type::type::INT64:
		return std::make_pair(GDF_INT64, gdf_dtype_extra_info{TIME_UNIT_NONE});
	case parquet::Type::type::FLOAT:
		return std::make_pair(GDF_FLOAT32, gdf_dtype_extra_info{TIME_UNIT_NONE});
	case parquet::Type::type::DOUBLE:
		return std::make_pair(GDF_FLOAT64, gdf_dtype_extra_info{TIME_UNIT_NONE});
	case parquet::Type::type::BYTE_ARRAY:
	case parquet::Type::type::FIXED_LEN_BYTE_ARRAY:
		// Can be mapped to GDF_CATEGORY (32-bit hash) or GDF_STRING (nvstring)
		// TODO: check GDF_STRING_CATEGORY
		return std::make_pair(strings_to_categorical ? GDF_CATEGORY : GDF_STRING,
							  gdf_dtype_extra_info{TIME_UNIT_NONE});
	case parquet::Type::type::INT96:
		// Convert Spark INT96 timestamp to GDF_DATE64
		return std::make_pair(GDF_DATE64, gdf_dtype_extra_info{TIME_UNIT_ms});
	default:
		break;
	}

	return std::make_pair(GDF_invalid, gdf_dtype_extra_info{TIME_UNIT_NONE});
}

// gdf_column* create_gdf_column(gdf_dtype dtype, gdf_dtype_extra_info dtype_info, const std::string &name) {
// 	auto column = new gdf_column{};
// 	std::string *clone_str = new std::string(name);
// 	gdf_error err = gdf_column_view_augmented(column, nullptr, nullptr, 0, dtype, 0, dtype_info, clone_str->c_str());
// 	return column;
// }

void set_gdf_column(gdf_column_cpp &p_column, std::vector<int64_t> & vector, unsigned long size) {
	size_t width_per_value = ral::traits::get_dtype_size_in_bytes(p_column.dtype());
	if (vector.size() != 0) {
		RMM_TRY(RMM_ALLOC(reinterpret_cast<void **>(&p_column.get_gdf_column()->data),  width_per_value * size, 0));
		CheckCudaErrors(cudaMemcpy(p_column.get_gdf_column()->data, vector.data(), width_per_value * size, cudaMemcpyHostToDevice));
		p_column.get_gdf_column()->size = size;
	} else {
		RMM_TRY(RMM_ALLOC(reinterpret_cast<void **>(&p_column.get_gdf_column()->data),  width_per_value * size, 0));
		// CheckCudaErrors(cudaMemcpy(p_column.get_gdf_column()->data, vector.data(), width_per_value * size, cudaMemcpyHostToDevice));
		p_column.get_gdf_column()->size = size;
	}
}

std::vector<gdf_column_cpp> get_minmax_metadata(
	std::vector<std::unique_ptr<parquet::ParquetFileReader>> &parquet_readers,
	size_t total_num_row_groups) {

	if (parquet_readers.size() == 0)
		return {};

	std::vector<std::vector<int64_t>> minmax_metadata_table;
	std::vector<gdf_column_cpp> minmax_metadata_gdf_table;

	std::shared_ptr<parquet::FileMetaData> file_metadata = parquet_readers[0]->metadata();

	// initialize minmax_metadata_table
	// T(min, max), (file_handle, row_group)
	minmax_metadata_table.resize(file_metadata->num_columns() * 2 + 2);
	minmax_metadata_gdf_table.resize(file_metadata->num_columns() * 2 + 2);

	int num_row_groups = file_metadata->num_row_groups();
	const parquet::SchemaDescriptor *schema = file_metadata->schema();

	if (num_row_groups > 0) {
		auto row_group_index = 0;
		auto groupReader = parquet_readers[0]->RowGroup(row_group_index);
		auto *rowGroupMetadata = groupReader->metadata();
		for (int colIndex = 0; colIndex < file_metadata->num_columns(); colIndex++) {
			const parquet::ColumnDescriptor *column = schema->Column(colIndex);
			auto columnMetaData = rowGroupMetadata->ColumnChunk(colIndex);
			auto physical_type = column->physical_type();
			auto logical_type = column->converted_type();
			gdf_dtype dtype;
			gdf_dtype_extra_info extra_info;
			std::tie(dtype, extra_info) = to_dtype(physical_type, logical_type);
			std::cout << "colIndex: " << colIndex  << "|"<< dtype << std::endl;

			if (dtype == GDF_CATEGORY || dtype == GDF_STRING || dtype == GDF_STRING_CATEGORY)
				dtype = GDF_INT32;

			auto col_name_min = "min_" + std::to_string(colIndex) + "_" + column->name();
			minmax_metadata_gdf_table[2 * colIndex].create_empty(dtype, col_name_min, extra_info.time_unit);
			
			auto col_name_max = "max_" + std::to_string(colIndex)  + "_" + column->name();
			minmax_metadata_gdf_table[2 * colIndex + 1].create_empty(dtype, col_name_max, extra_info.time_unit);
		}

		
		minmax_metadata_gdf_table[minmax_metadata_gdf_table.size() - 2].create_empty(GDF_INT32, "file_handle_index", TIME_UNIT_NONE);
		minmax_metadata_gdf_table[minmax_metadata_gdf_table.size() - 1].create_empty(GDF_INT32, "row_group_index", TIME_UNIT_NONE);
	}

	size_t file_index = 0;
	std::vector<std::thread> threads(parquet_readers.size());
	std::mutex guard;
	for (size_t file_index = 0; file_index < parquet_readers.size(); file_index++){
		threads[file_index] = std::thread([&guard, &parquet_readers, file_index, &minmax_metadata_table ](){

		  std::shared_ptr<parquet::FileMetaData> file_metadata = parquet_readers[file_index]->metadata();

		  int num_row_groups = file_metadata->num_row_groups();
		  const parquet::SchemaDescriptor *schema = file_metadata->schema();

		  for (int row_group_index = 0; row_group_index < num_row_groups;
			   row_group_index++) {
			  auto groupReader = parquet_readers[file_index]->RowGroup(row_group_index);
			  auto *rowGroupMetadata = groupReader->metadata();
			  for (int colIndex = 0; colIndex < file_metadata->num_columns();
				   colIndex++) {
				  const parquet::ColumnDescriptor *column = schema->Column(colIndex);
				  auto columnMetaData = rowGroupMetadata->ColumnChunk(colIndex);
				  if (columnMetaData->is_stats_set()) {
					  auto statistics = columnMetaData->statistics();
					  if (statistics->HasMinMax()) {
						  guard.lock();
						  set_min_max(minmax_metadata_table,
						  	colIndex * 2,
							  	     column->physical_type(),
							  	     column->converted_type(),
									 statistics);
						  guard.unlock();
					  }
				  }
			  }
			  guard.lock();
			  //TODO change to file_handle index, Use file_handle_offset,  row_group_index_offset?
			  minmax_metadata_table[minmax_metadata_table.size() - 2].push_back(file_index);
			  minmax_metadata_table[minmax_metadata_table.size() - 1].push_back(row_group_index);
			  guard.unlock();
		  }
		});
	}

	for (size_t file_index = 0; file_index < parquet_readers.size(); file_index++){
		threads[file_index].join();
	}
	for (size_t index = 0; index < 	minmax_metadata_table.size(); index++) {
		set_gdf_column(minmax_metadata_gdf_table[index], minmax_metadata_table[index], num_row_groups);
	}
	return minmax_metadata_gdf_table;
}
#endif	// BLAZINGDB_RAL_SRC_IO_DATA_PARSER_METADATA_PARQUET_METADATA_CPP_H_
