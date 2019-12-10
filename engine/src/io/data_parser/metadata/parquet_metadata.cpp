//
// Created by aocsa on 12/9/19.
//

#ifndef BLAZINGDB_RAL_SRC_IO_DATA_PARSER_METADATA_PARQUET_METADATA_CPP_H_
#define BLAZINGDB_RAL_SRC_IO_DATA_PARSER_METADATA_PARQUET_METADATA_CPP_H_

#include "parquet_metadata.h"


void set_min_max(
	std::vector<std::vector<int64_t>> &minmax_metadata_table,
	int col_index, parquet::Type::type physical,
	parquet::ConvertedType::type logical,
	std::shared_ptr<parquet::RowGroupStatistics> &statistics) {

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
		auto min = convertedStats.min();
		auto max = convertedStats.max();
		minmax_metadata_table[col_index]->push_back(&min);
		minmax_metadata_table[col_index + 1]->push_back(&max);
		break;
	}
	case parquet::ConvertedType::type::TIMESTAMP_MICROS: {
		auto convertedStats =
			std::static_pointer_cast<parquet::Int64Statistics>(statistics);
		// gdf_dtype_extra_info{TIME_UNIT_us}
		// TODO, review this case
		auto min = convertedStats.min();
		auto max = convertedStats.max();
		minmax_metadata_table[col_index]->push_back(&min);
		minmax_metadata_table[col_index + 1]->push_back(&max);
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
		auto min = convertedStats.min();
		auto max = convertedStats.max();
		minmax_metadata_table[col_index]->push_back(&min);
		minmax_metadata_table[col_index + 1]->push_back(&max);
		break;
	}
	case parquet::Type::type::INT32: {
		auto convertedStats =
			std::static_pointer_cast<parquet::Int32Statistics>(statistics);
		auto min = convertedStats.min();
		auto max = convertedStats.max();
		minmax_metadata_table[col_index]->push_back(&min);
		minmax_metadata_table[col_index + 1]->push_back(&max);

		break;
	}
	case parquet::Type::type::INT64: {
		auto convertedStats =
			std::static_pointer_cast<parquet::Int64Statistics>(statistics);
		auto min = convertedStats.min();
		auto max = convertedStats.max();
		minmax_metadata_table[col_index]->push_back(&min);
		minmax_metadata_table[col_index + 1]->push_back(&max);
		break;
	}
	case parquet::Type::type::FLOAT: {
		auto convertedStats =
			std::static_pointer_cast<parquet::FloatStatistics>(statistics);
		auto min = convertedStats.min();
		auto max = convertedStats.max();
		minmax_metadata_table[col_index]->push_back(&min);
		minmax_metadata_table[col_index + 1]->push_back(&max);
		break;
	}
	case parquet::Type::type::DOUBLE: {
		auto convertedStats =
			std::static_pointer_cast<parquet::DoubleStatistics>(statistics);
		auto min = convertedStats.min();
		auto max = convertedStats.max();
		minmax_metadata_table[col_index]->push_back(&min);
		minmax_metadata_table[col_index + 1]->push_back(&max);
		break;
	}
	case parquet::Type::type::BYTE_ARRAY:
	case parquet::Type::type::FIXED_LEN_BYTE_ARRAY: {
		auto convertedStats =
			std::static_pointer_cast<parquet::FLBAStatistics>(statistics);
		// TODO: willian
		std::string min = "";
		std::string max = "";
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

std::vector<gdf_column*> get_minmax_metadata(
	std::vector<std::unique_ptr<parquet::ParquetFileReader>> &parquet_readers,
	const std::vector<std::string> &user_readable_file_handles,
	size_t total_num_row_groups) {

//	auto id = create_id(user_readable_file_handles);
//	auto output = retrieve_min_max_metadata(id);
//	if (output.size() != 0) {
//		return output;
//	}

	std::vector<std::vector<int64_t>> minmax_metadata_table;
	if (parquet_readers.size() == 0)
		return minmax_metadata_table;


	std::shared_ptr<parquet::FileMetaData> file_metadata =
		parquet_readers[0]->metadata();

	// initialize minmax_metadata_table
	// T(min, max), (file_handle, row_group)
	minmax_metadata_table.resize(file_metadata->num_columns() * 2 + 2);

	int num_row_groups = file_metadata->num_row_groups();
	const parquet::SchemaDescriptor *schema = file_metadata->schema();

	if (num_row_groups > 0) {
		auto row_group_index = 0;
		auto groupReader = parquet_readers[0]->RowGroup(row_group_index);
		auto *rowGroupMetadata = groupReader->metadata();

		for (int colIndex = 0; colIndex < file_metadata->num_columns();
			 colIndex++) {
			std::cout << "colIndex: " << colIndex << std::endl;
			const parquet::ColumnDescriptor *column = schema->Column(colIndex);
			auto columnMetaData = rowGroupMetadata->ColumnChunk(colIndex);
			auto physical_type = column->physical_type();
			auto logical_type = column->converted_type();
			gdf_dtype dtype;
			gdf_dtype_extra_info extra_info;
			std::tie(dtype, extra_info) = to_dtype(physical_type, logical_type);
			auto col_name_min = "min_" + column->name();

			minmax_metadata_table[2 * colIndex] =
				create_host_column(dtype, extra_info, col_name_min);

			auto col_name_max = "max_" + column->name();
			minmax_metadata_table[2 * colIndex + 1] =
				create_host_column(dtype, extra_info, col_name_max);
		}
		minmax_metadata_table[minmax_metadata_table.size() - 2] =
			create_host_column(GDF_STRING, gdf_dtype_extra_info{
				TIME_UNIT_NONE}, "file_handle"); // file_handle
		minmax_metadata_table[minmax_metadata_table.size() - 1] =
			create_host_column(
				GDF_INT32, gdf_dtype_extra_info{TIME_UNIT_NONE}, "row_group"); // row_group
	}

	size_t file_index = 0;
	std::vector<std::thread> threads(parquet_readers.size());
	std::mutex guard;
	for (size_t file_index = 0; file_index < parquet_readers.size(); file_index++){
		std::string file_handle = user_readable_file_handles[file_index];
		threads[file_index] = std::thread([&guard, &parquet_readers, file_index, file_handle, &minmax_metadata_table ](){

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
						  set_min_max(minmax_metadata_table, colIndex * 2,
									  column->physical_type(), column->converted_type(),
									  statistics);
						  guard.unlock();
					  }
				  }
			  }
			  guard.lock();
			  minmax_metadata_table[minmax_metadata_table.size() - 2]->push_back(
				  file_handle.c_str());
			  minmax_metadata_table[minmax_metadata_table.size() - 1]->push_back(
				  &row_group_index);
			  guard.unlock();
		  }
		});
	}

	for (size_t file_index = 0; file_index < parquet_readers.size(); file_index++){
		threads[file_index].join();
	}

	return minmax_metadata_table;
}
#endif	// BLAZINGDB_RAL_SRC_IO_DATA_PARSER_METADATA_PARQUET_METADATA_CPP_H_
