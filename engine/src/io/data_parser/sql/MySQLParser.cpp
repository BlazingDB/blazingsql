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

#include "Util/StringUtil.h"

#include <cudf/io/parquet.hpp>

#include <mysql/jdbc.h>

namespace ral {
namespace io {

namespace cudf_io = cudf::io;

cudf::io::table_with_metadata read_mysql(std::shared_ptr<sql::ResultSet> res) {
  cudf::io::table_with_metadata ret;
  //ret.metadata.column_names = std::move(column_names);
  // TODO percy  
}

// TODO percy this is too naive ... improve this later
//String Data Types
//Data type	Description
//CHAR(size)	A FIXED length string (can contain letters, numbers, and special characters). The size parameter specifies the column length in characters - can be from 0 to 255. Default is 1
//VARCHAR(size)	A VARIABLE length string (can contain letters, numbers, and special characters). The size parameter specifies the maximum column length in characters - can be from 0 to 65535
//BINARY(size)	Equal to CHAR(), but stores binary byte strings. The size parameter specifies the column length in bytes. Default is 1
//VARBINARY(size)	Equal to VARCHAR(), but stores binary byte strings. The size parameter specifies the maximum column length in bytes.
//TINYBLOB	For BLOBs (Binary Large OBjects). Max length: 255 bytes
//TINYTEXT	Holds a string with a maximum length of 255 characters
//TEXT(size)	Holds a string with a maximum length of 65,535 bytes
//BLOB(size)	For BLOBs (Binary Large OBjects). Holds up to 65,535 bytes of data
//MEDIUMTEXT	Holds a string with a maximum length of 16,777,215 characters
//MEDIUMBLOB	For BLOBs (Binary Large OBjects). Holds up to 16,777,215 bytes of data
//LONGTEXT	Holds a string with a maximum length of 4,294,967,295 characters
//LONGBLOB	For BLOBs (Binary Large OBjects). Holds up to 4,294,967,295 bytes of data
//ENUM(val1, val2, val3, ...)	A string object that can have only one value, chosen from a list of possible values. You can list up to 65535 values in an ENUM list. If a value is inserted that is not in the list, a blank value will be inserted. The values are sorted in the order you enter them
//SET(val1, val2, val3, ...)
bool is_cudf_string(const std::string mysql_column_type) {
  std::vector<std::string> mysql_string_types_hints = {
    "CHAR",
    "VARCHAR",
    "BINARY",
    "VARBINARY",
    "TINYBLOB",
    "TINYTEXT",
    "TEXT",
    "BLOB",
    "MEDIUMTEXT",
    "MEDIUMBLOB",
    "LONGTEXT",
    "LONGBLOB",
    "ENUM",
    "SET"
  };

  for (auto hint : mysql_string_types_hints) {
    if (StringUtil::beginsWith(mysql_column_type, hint)) {
      return true;
    }  
  }
  
  return false;
}

cudf::type_id parse_column_type(const std::string mysql_column_type) {
  if (is_cudf_string(mysql_column_type)) return cudf::type_id::STRING;
  // TODO percy ...
}

mysql_parser::mysql_parser() {
}

mysql_parser::~mysql_parser() {
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
    result.metadata.column_names = std::move(col_names);

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
	for(int i = 0; i < handle.sql_handle.column_names.size(); i++) {
		cudf::type_id type = parse_column_type(handle.sql_handle.column_types.at(i));
		size_t file_index = i;
		bool is_in_file = true;
		std::string name = handle.sql_handle.column_names.at(i);
		schema.add_column(name, type, file_index, is_in_file);
	}
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
