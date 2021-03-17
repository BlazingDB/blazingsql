/*
 * Copyright 2021 BlazingDB, Inc.
 *     Copyright 2021 Percy Camilo Triveño Aucahuasi <percy@blazingdb.com>
 */

#include "MySQLParser.h"

#include "utilities/CommonOperations.h"

#include <numeric>

#include <arrow/io/file.h>
#include "ExceptionHandling/BlazingThread.h"

#include <parquet/column_writer.h>
#include <parquet/file_writer.h>

#include <cudf/io/parquet.hpp>

#include <mysql/jdbc.h>

namespace ral {
namespace io {

namespace cudf_io = cudf::io;

mysql_parser::mysql_parser() {
}

mysql_parser::~mysql_parser() {
}

cudf::io::table_with_metadata read_mysql(std::shared_ptr<sql::ResultSet> res) {
  
}

std::unique_ptr<ral::frame::BlazingTable> mysql_parser::parse_batch(
	ral::io::data_handle handle,
	const Schema & schema,
	std::vector<int> column_indices,
	std::vector<cudf::size_type> row_groups) 
{
  auto res = handle.sql_handle.mysql_resultset;
	if(res == nullptr) {
		return schema.makeEmptyBlazingTable(column_indices);
	}
	if(column_indices.size() > 0) {
		std::vector<std::string> col_names(column_indices.size());

		for(size_t column_i = 0; column_i < column_indices.size(); column_i++) {
			col_names[column_i] = schema.get_name(column_indices[column_i]);
		}

		auto result = read_mysql(res);

		auto result_table = std::move(result.tbl);
		if (result.metadata.column_names.size() > column_indices.size()) {
			auto columns = result_table->release();
			// Assuming columns are in the same order as column_indices and any extra columns (i.e. index column) are put last
			columns.resize(column_indices.size());
			result_table = std::make_unique<cudf::table>(std::move(columns));
		}

		return std::make_unique<ral::frame::BlazingTable>(std::move(result_table), result.metadata.column_names);
	}
	return nullptr;
}

void mysql_parser::parse_schema(ral::io::data_handle handle, ral::io::Schema & schema) {

//	cudf_io::table_with_metadata table_out = cudf_io::read_parquet(pq_args);

//	for(int i = 0; i < table_out.tbl->num_columns(); i++) {
//		cudf::type_id type = table_out.tbl->get_column(i).type().id();
//		size_t file_index = i;
//		bool is_in_file = true;
//		std::string name = table_out.metadata.column_names.at(i);
//		schema.add_column(name, type, file_index, is_in_file);
//	}
}

std::unique_ptr<ral::frame::BlazingTable> mysql_parser::get_metadata(
	std::vector<ral::io::data_handle> handles, int offset){
//	std::vector<size_t> num_row_groups(files.size());
//	BlazingThread threads[files.size()];
//	std::vector<std::unique_ptr<parquet::ParquetFileReader>> parquet_readers(files.size());
//	for(size_t file_index = 0; file_index < files.size(); file_index++) {
//		threads[file_index] = BlazingThread([&, file_index]() {
//		  parquet_readers[file_index] =
//			  std::move(parquet::ParquetFileReader::Open(files[file_index]));
//		  std::shared_ptr<parquet::FileMetaData> file_metadata = parquet_readers[file_index]->metadata();
//		  num_row_groups[file_index] = file_metadata->num_row_groups();
//		});
//	}

//	for(size_t file_index = 0; file_index < files.size(); file_index++) {
//		threads[file_index].join();
//	}

//	size_t total_num_row_groups =
//		std::accumulate(num_row_groups.begin(), num_row_groups.end(), size_t(0));

//	auto minmax_metadata_table = get_minmax_metadata(parquet_readers, total_num_row_groups, offset);
//	for (auto &reader : parquet_readers) {
//		reader->Close();
//	}
//	return minmax_metadata_table;
}

} /* namespace io */
} /* namespace ral */
