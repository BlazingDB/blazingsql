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
#include <src/Traits/RuntimeTraits.h>
#include <from_cudf/cpp_src/utilities/legacy/error_utils.hpp>
#include <thread>

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
cudf::type_id to_dtype(parquet::Type::type physical, parquet::ConvertedType::type logical) {

	// Logical type used for actual data interpretation; the legacy converted type
	// is superceded by 'logical' type whenever available.
	switch (logical) {
	case parquet::ConvertedType::type::UINT_8:
	case parquet::ConvertedType::type::INT_8:
		return cudf::type_id::INT8;
	case parquet::ConvertedType::type::UINT_16:
	case parquet::ConvertedType::type::INT_16:
		return cudf::type_id::INT16;
	case parquet::ConvertedType::type::DATE:
		return cudf::type_id::TIMESTAMP_DAYS;
	case parquet::ConvertedType::type::TIMESTAMP_MILLIS:
		return cudf::type_id::TIMESTAMP_MILLISECONDS;
	case parquet::ConvertedType::type::TIMESTAMP_MICROS:
		return cudf::type_id::TIMESTAMP_MICROSECONDS;
	default:
		break;
	}

	// Physical storage type supported by Parquet; controls the on-disk storage
	// format in combination with the encoding type.
	switch (physical) {
	case parquet::Type::type::BOOLEAN:
		return cudf::type_id::BOOL8;
	case parquet::Type::type::INT32:
		return cudf::type_id::INT32;
	case parquet::Type::type::INT64:
		return cudf::type_id::INT64;
	case parquet::Type::type::FLOAT:
		return cudf::type_id::FLOAT32;
	case parquet::Type::type::DOUBLE:
		return cudf::type_id::FLOAT64;
	case parquet::Type::type::BYTE_ARRAY:
	case parquet::Type::type::FIXED_LEN_BYTE_ARRAY:
		// TODO: Check GDF_STRING_CATEGORY
		return cudf::type_id::STRING;
	case parquet::Type::type::INT96:
	default:
		break;
	}

	return cudf::type_id::EMPTY;
}


std::basic_string<char> get_typed_vector_content(cudf::type_id dtype, const std::vector<int64_t> &vector) {
  std::basic_string<char> output;
  switch (dtype) {
	case cudf::type_id::INT8:{
			std::vector<char> typed_v(vector.begin(), vector.end());
			output = std::basic_string<char>((char *)typed_v.data(), typed_v.size() * sizeof(char));
			break;
		}
	case cudf::type_id::INT16: {
		std::vector<int16_t> typed_v(vector.begin(), vector.end());
		output = std::basic_string<char>((char *)typed_v.data(), typed_v.size() * sizeof(int16_t));
		break;
	}
	case cudf::type_id::INT32:{
		std::vector<int32_t> typed_v(vector.begin(), vector.end());
		output = std::basic_string<char>((char *)typed_v.data(), typed_v.size() * sizeof(int32_t));
		break;
	}
	case cudf::type_id::INT64: {
		std::vector<int64_t> typed_v(vector.begin(), vector.end());
		output = std::basic_string<char>((char *)typed_v.data(), typed_v.size() * sizeof(int64_t));
		break;
	}
	case cudf::type_id::FLOAT32: {
		std::vector<float> typed_v(vector.begin(), vector.end());
		output = std::basic_string<char>((char *)typed_v.data(), typed_v.size() * sizeof(float));
		break;
	}
	case cudf::type_id::FLOAT64: {
		std::vector<double> typed_v(vector.begin(), vector.end());
		output = std::basic_string<char>((char *)typed_v.data(), typed_v.size() * sizeof(double));
		break;
	}
	case cudf::type_id::BOOL8: {
		std::vector<int8_t> typed_v(vector.begin(), vector.end());
		output = std::basic_string<char>((char *)typed_v.data(), typed_v.size() * sizeof(int8_t));
		break;
	}
	case cudf::type_id::TIMESTAMP_DAYS: {
		std::vector<int32_t> typed_v(vector.begin(), vector.end());
		output = std::basic_string<char>((char *)typed_v.data(), typed_v.size() * sizeof(int32_t));
		break;
	}
	case cudf::type_id::TIMESTAMP_SECONDS: {
		std::vector<int64_t> typed_v(vector.begin(), vector.end());
		output = std::basic_string<char>((char *)typed_v.data(), typed_v.size() * sizeof(int64_t));
		break;
	}
	case cudf::type_id::TIMESTAMP_MILLISECONDS: {
		std::vector<int64_t> typed_v(vector.begin(), vector.end());
		output = std::basic_string<char>((char *)typed_v.data(), typed_v.size() * sizeof(int64_t));
		break;
	}
	case cudf::type_id::TIMESTAMP_MICROSECONDS: {
		std::vector<int64_t> typed_v(vector.begin(), vector.end());
		output = std::basic_string<char>((char *)typed_v.data(), typed_v.size() * sizeof(int64_t));
		break;
	}
	case cudf::type_id::TIMESTAMP_NANOSECONDS: {
		std::vector<int64_t> typed_v(vector.begin(), vector.end());
		output = std::basic_string<char>((char *)typed_v.data(), typed_v.size() * sizeof(int64_t));
		break;
	}
	default: {
		// default return type since we're throwing an exception.
		std::cerr << "Invalid gdf_dtype in create_host_column" << std::endl;
		throw std::runtime_error("Invalid gdf_dtype in create_host_column");
	}
  }
  return output;
}

std::unique_ptr<cudf::column> make_cudf_column_from(cudf::data_type dtype, std::basic_string<char> &vector, unsigned long column_size) {
	size_t width_per_value = cudf::size_of(dtype);
	if (vector.size() != 0) {
		auto buffer_size = width_per_value * column_size;
		rmm::device_buffer gpu_buffer(vector.data(), buffer_size);
		return std::make_unique<cudf::column>(dtype, column_size, std::move(gpu_buffer));
	} else {
		auto buffer_size = width_per_value * column_size;
		rmm::device_buffer gpu_buffer(buffer_size);
		return std::make_unique<cudf::column>(dtype, column_size, buffer_size);
	}
}

std::unique_ptr<cudf::column> make_empty_column(cudf::data_type type) {
  return std::make_unique<cudf::column>(type, 0, rmm::device_buffer{});
}

std::unique_ptr<ral::frame::BlazingTable> get_minmax_metadata(
	std::vector<std::unique_ptr<parquet::ParquetFileReader>> &parquet_readers,
	size_t total_num_row_groups, int metadata_offset) {

	if (parquet_readers.size() == 0){
		return nullptr;
	}
	std::vector<std::vector<int64_t>> minmax_metadata_table;
	std::vector<std::string> metadata_names;
	std::vector<cudf::data_type> metadata_dtypes;

	std::shared_ptr<parquet::FileMetaData> file_metadata = parquet_readers[0]->metadata();

	// initialize minmax_metadata_table
	// T(min, max), (file_handle, row_group)
	minmax_metadata_table.resize(file_metadata->num_columns() * 2 + 2);
	metadata_names.resize(file_metadata->num_columns() * 2 + 2);
	metadata_dtypes.resize(file_metadata->num_columns() * 2 + 2);

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
			cudf::data_type dtype = cudf::data_type (to_dtype(physical_type, logical_type)) ;

			if (dtype.id() == cudf::type_id::STRING )
				dtype = cudf::data_type(cudf::type_id::INT32);

			auto col_name_min = "min_" + std::to_string(colIndex) + "_" + column->name();
			metadata_dtypes[2 * colIndex] = dtype;
			metadata_names[2 * colIndex] = col_name_min;
			
			auto col_name_max = "max_" + std::to_string(colIndex)  + "_" + column->name();
			metadata_dtypes[2 * colIndex + 1] = dtype;
			metadata_names[2 * colIndex + 1] = col_name_max;
		}

		metadata_dtypes[metadata_dtypes.size() - 2] = cudf::data_type{cudf::type_id::INT32};
		metadata_names[metadata_names.size() - 2] = "file_handle_index";
		metadata_dtypes[metadata_dtypes.size() - 1] = cudf::data_type{cudf::type_id::INT32};
		metadata_names[metadata_names.size() - 1] = "row_group_index";
	}

	size_t file_index = 0;
	std::vector<std::thread> threads(parquet_readers.size());
	std::mutex guard;
	for (size_t file_index = 0; file_index < parquet_readers.size(); file_index++){
		// NOTE: It is really important to mantain the `file_index order` in order to match the same order in HiveMetadata
		// threads[file_index] = std::thread([&guard, metadata_offset,  &parquet_readers, file_index, &minmax_metadata_table ](){
		  std::shared_ptr<parquet::FileMetaData> file_metadata = parquet_readers[file_index]->metadata();

		  int num_row_groups = file_metadata->num_row_groups();
		  const parquet::SchemaDescriptor *schema = file_metadata->schema();

		  for (int row_group_index = 0; row_group_index < num_row_groups; row_group_index++) {
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
			  minmax_metadata_table[minmax_metadata_table.size() - 2].push_back(metadata_offset + file_index);
			  minmax_metadata_table[minmax_metadata_table.size() - 1].push_back(row_group_index);
			  guard.unlock();
		  }
		// });
	}
	// NOTE: It is really important to mantain the `file_index order` in order to match the same order in HiveMetadata
	// for (size_t file_index = 0; file_index < parquet_readers.size(); file_index++){
	// 	threads[file_index].join();
	// }

	std::vector<std::unique_ptr<cudf::column>> minmax_metadata_gdf_table(file_metadata->num_columns() * 2 + 2);
	for (size_t index = 0; index < 	minmax_metadata_table.size(); index++) {
		auto vector = minmax_metadata_table[index];
		auto dtype = metadata_dtypes[index];
		auto content =  get_typed_vector_content(dtype.id(), vector);
		minmax_metadata_gdf_table[index] = make_cudf_column_from(dtype, content, total_num_row_groups);
	}
	auto table = std::make_unique<cudf::experimental::table>(std::move(minmax_metadata_gdf_table));
	return std::make_unique<ral::frame::BlazingTable>(std::move(table), metadata_names);
}
#endif	// BLAZINGDB_RAL_SRC_IO_DATA_PARSER_METADATA_PARQUET_METADATA_CPP_H_
