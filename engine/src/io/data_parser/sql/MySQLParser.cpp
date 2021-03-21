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
#include <cudf/strings/convert/convert_datetime.hpp>

#include "Util/StringUtil.h"

#include <cudf/io/parquet.hpp>
#include <cudf_test/column_wrapper.hpp>

#include <mysql/jdbc.h>

namespace ral {
namespace io {

namespace cudf_io = cudf::io;

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
bool mysql_is_cudf_string(const std::string &t) {
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
    if (StringUtil::beginsWith(t, hint)) return true;
  }

  return false;
}

cudf::type_id parse_mysql_column_type(const std::string t) {
  if (mysql_is_cudf_string(t)) return cudf::type_id::STRING;
  // test numeric data types ...
  if (StringUtil::beginsWith(t, "BOOL") || 
      StringUtil::beginsWith(t, "BOOLEAN")) return cudf::type_id::BOOL8;
  if (StringUtil::beginsWith(t, "TINYINT")) return cudf::type_id::INT8;
  if (StringUtil::beginsWith(t, "INT") || 
      StringUtil::beginsWith(t, "INTEGER")) return cudf::type_id::INT32;
  if (StringUtil::beginsWith(t, "BIGINT")) return cudf::type_id::INT64;
  if (StringUtil::beginsWith(t, "FLOAT")) return cudf::type_id::FLOAT32;
  if (StringUtil::beginsWith(t, "DOUBLE")) return cudf::type_id::FLOAT64;
  // test date/datetime data types ...
  if (t == "DATE") return cudf::type_id::TIMESTAMP_DAYS;
  if (t == "TIME") return cudf::type_id::TIMESTAMP_SECONDS; // without the date part
  if (StringUtil::beginsWith(t, "DATETIME")) return cudf::type_id::TIMESTAMP_MILLISECONDS;
  if (StringUtil::beginsWith(t, "TIMESTAMP")) return cudf::type_id::TIMESTAMP_MILLISECONDS;
  if (StringUtil::beginsWith(t, "YEAR")) return cudf::type_id::INT8;


  // TODO percy ...
}

std::vector<cudf::type_id> parse_mysql_column_types(const std::vector<std::string> types) {
  std::vector<cudf::type_id> ret;
  for (auto t : types) {
    ret.push_back(parse_mysql_column_type(t));
  }
  return ret;
}

template<typename T>
std::unique_ptr<cudf::column> build_fixed_width_cudf_col(size_t total_rows,
                                                         char *host_col,
                                                         const std::vector<uint8_t> &valids)
{
  T *cols_buff = (T*)host_col;
  std::vector<T> cols(cols_buff, cols_buff + total_rows);
  cudf::test::fixed_width_column_wrapper<T> vals(cols.begin(), cols.end(), valids.begin());
  return vals.release();
}

std::unique_ptr<cudf::column> build_str_cudf_col(size_t total_rows,
                                                 char *host_col,
                                                 size_t data_size,
                                                 const std::vector<uint8_t> &valids)
{
  char *buff[total_rows];
  for (int i = 0; i < total_rows; ++i) {
      buff[i] = host_col + i*data_size;
  }
  cudf::test::strings_column_wrapper vals(buff, buff+total_rows, valids.begin());
  return std::move(vals.release());
}

cudf::io::table_with_metadata read_mysql(std::shared_ptr<sql::ResultSet> res,
                                         const std::vector<int> &column_indices,
                                         const std::vector<cudf::type_id> &cudf_types,
                                         const std::vector<size_t> &column_bytes,
                                         size_t total_rows)
{
  cudf::io::table_with_metadata ret;
  std::vector<char*> host_cols(column_indices.size());
  std::vector<std::vector<uint8_t>> host_valids(host_cols.size());

  for (int col = 0; col < host_cols.size(); ++col) {
    size_t projection = column_indices[col];
    size_t len = total_rows*column_bytes[projection];
    host_cols[col] = (char*)malloc(len);
    host_valids[col].resize(total_rows);
  }

  int row = 0;
  while (res->next()) {
    for (int col = 0; col < column_indices.size(); ++col) {
      int mysql_col = col + 1; // mysql jdbc getString starts counting from 1 (not from 0)
      size_t projection = column_indices[col];
      size_t data_size = column_bytes[projection];
      size_t offset = row*data_size;
      uint8_t valid = 1; // 1: not null 0: null
      cudf::type_id cudf_type_id = cudf_types[projection];

      switch (cudf_type_id) {
        case cudf::type_id::EMPTY: {
        } break;
        case cudf::type_id::INT8:
        case cudf::type_id::INT16:
        case cudf::type_id::INT32: {
          char *data = data = (char*)res->getInt(mysql_col);
          if (res->wasNull()) {
            valid = 0;
          } else {
            strncpy(host_cols[col] + offset, (char*)(&data), data_size);
          }
        } break;
        case cudf::type_id::INT64: {
          char *data = data = (char*)res->getInt64(mysql_col);
          if (res->wasNull()) {
            valid = 0;
          } else {
            strncpy(host_cols[col] + offset, (char*)(&data), data_size);
          }
        } break;
        case cudf::type_id::UINT8:
        case cudf::type_id::UINT16:
        case cudf::type_id::UINT32: {
          char *data = data = (char*)res->getUInt(mysql_col);
          if (res->wasNull()) {
            valid = 0;
          } else {
            strncpy(host_cols[col] + offset, (char*)(&data), data_size);
          }
        } break;
        case cudf::type_id::UINT64: {
          char *data = data = (char*)res->getUInt64(mysql_col);
          if (res->wasNull()) {
            valid = 0;
          } else {
            strncpy(host_cols[col] + offset, (char*)(&data), data_size);
          }
        } break;
        case cudf::type_id::FLOAT32:
        case cudf::type_id::FLOAT64: {
          char *data = data = reinterpret_cast<char*>(res->getDouble(mysql_col), sizeof(long double));
          if (res->wasNull()) {
            valid = 0;
          } else {
            strncpy(host_cols[col] + offset, (char*)(&data), data_size);
          }
        } break;
        case cudf::type_id::BOOL8: {
          char *data = data = (char*)res->getBoolean(mysql_col);
          if (res->wasNull()) {
            valid = 0;
          } else {
            strncpy(host_cols[col] + offset, (char*)(&data), data_size);
          }
        } break;
        case cudf::type_id::TIMESTAMP_DAYS:
        case cudf::type_id::TIMESTAMP_SECONDS:
        case cudf::type_id::TIMESTAMP_MILLISECONDS:
        case cudf::type_id::TIMESTAMP_MICROSECONDS:
        case cudf::type_id::TIMESTAMP_NANOSECONDS:
// TODO percy it seems we don't support this yet
//        case cudf::type_id::DURATION_DAYS: {
          
//        } break;
//        case cudf::type_id::DURATION_SECONDS: {
          
//        } break;
//        case cudf::type_id::DURATION_MILLISECONDS: {
          
//        } break;
//        case cudf::type_id::DURATION_MICROSECONDS: {
          
//        } break;
//        case cudf::type_id::DURATION_NANOSECONDS: {
          
//        } break;
//        case cudf::type_id::DICTIONARY32: {
          
//        } break;
        case cudf::type_id::STRING: {
          auto mysql_str = res->getString(mysql_col);
          char *data = data = (char*)mysql_str.c_str();
          if (res->wasNull()) {
            valid = 0;
          } else {
            strncpy(host_cols[col] + offset, data, data_size);
          }
        } break;
        case cudf::type_id::LIST: {
          
        } break;
        case cudf::type_id::DECIMAL32: {
          
        } break;
        case cudf::type_id::DECIMAL64: {
          
        } break;
        case cudf::type_id::STRUCT: {
          
        } break;
      }

      host_valids[col][row] = valid;
    }
    ++row;
  }

  std::vector<std::unique_ptr<cudf::column>> cudf_cols(host_cols.size());

  for (int col = 0; col < column_indices.size(); ++col) {
    size_t projection = column_indices[col];
    size_t data_size = column_bytes[projection];
    cudf::type_id cudf_type_id = cudf_types[projection];

    uint8_t *valids_buff = host_valids[col].data();
    std::vector<uint8_t> valids(valids_buff, valids_buff + total_rows);

    switch (cudf_type_id) {
      case cudf::type_id::EMPTY: {
      } break;
      case cudf::type_id::INT8: {
        cudf_cols[col] = build_fixed_width_cudf_col<int8_t>(total_rows, host_cols[col], valids);
      } break;
      case cudf::type_id::INT16: {
        cudf_cols[col] = build_fixed_width_cudf_col<int16_t>(total_rows, host_cols[col], valids);
      } break;
      case cudf::type_id::INT32: {
        cudf_cols[col] = build_fixed_width_cudf_col<int32_t>(total_rows, host_cols[col], valids);
      } break;
      case cudf::type_id::INT64: {
        cudf_cols[col] = build_fixed_width_cudf_col<int64_t>(total_rows, host_cols[col], valids);
      } break;
      case cudf::type_id::UINT8: {
        cudf_cols[col] = build_fixed_width_cudf_col<uint8_t>(total_rows, host_cols[col], valids);
      } break;
      case cudf::type_id::UINT16: {
        cudf_cols[col] = build_fixed_width_cudf_col<uint16_t>(total_rows, host_cols[col], valids);
      } break;
      case cudf::type_id::UINT32: {
        cudf_cols[col] = build_fixed_width_cudf_col<uint32_t>(total_rows, host_cols[col], valids);
      } break;
      case cudf::type_id::UINT64: {
        cudf_cols[col] = build_fixed_width_cudf_col<uint64_t>(total_rows, host_cols[col], valids);
      } break;
      case cudf::type_id::FLOAT32: {
        cudf_cols[col] = build_fixed_width_cudf_col<float>(total_rows, host_cols[col], valids);
      } break;
      case cudf::type_id::FLOAT64: {
        cudf_cols[col] = build_fixed_width_cudf_col<double>(total_rows, host_cols[col], valids);
      } break;
      case cudf::type_id::BOOL8: {
        cudf_cols[col] = build_fixed_width_cudf_col<bool>(total_rows, host_cols[col], valids);
      } break;
      case cudf::type_id::TIMESTAMP_DAYS: {
        auto str_col = build_str_cudf_col(total_rows, host_cols[col], data_size, valids);
        auto vals_col = cudf::strings::to_timestamps(str_col->view(), cudf::data_type{cudf::type_id::TIMESTAMP_DAYS}, "%Y-%m-%d");
        cudf_cols[col] = std::move(vals_col);
      } break;
      case cudf::type_id::TIMESTAMP_SECONDS: {
        auto str_col = build_str_cudf_col(total_rows, host_cols[col], data_size, valids);
        auto vals_col = cudf::strings::to_timestamps(str_col->view(), cudf::data_type{cudf::type_id::TIMESTAMP_SECONDS}, "%H:%M:%S");
        cudf_cols[col] = std::move(vals_col);
      } break;
      case cudf::type_id::TIMESTAMP_MILLISECONDS: {
        auto str_col = build_str_cudf_col(total_rows, host_cols[col], data_size, valids);
        auto vals_col = cudf::strings::to_timestamps(str_col->view(), cudf::data_type{cudf::type_id::TIMESTAMP_MILLISECONDS}, "%Y-%m-%d %H:%M:%S");
        cudf_cols[col] = std::move(vals_col);
      } break;
      case cudf::type_id::TIMESTAMP_MICROSECONDS: {

      } break;
      case cudf::type_id::TIMESTAMP_NANOSECONDS: {
        
      } break;
        // TODO percy it seems we don't support this yet        
//      case cudf::type_id::DURATION_DAYS: {
        
//      } break;
//      case cudf::type_id::DURATION_SECONDS: {
        
//      } break;
//      case cudf::type_id::DURATION_MILLISECONDS: {
        
//      } break;
//      case cudf::type_id::DURATION_MICROSECONDS: {
        
//      } break;
//      case cudf::type_id::DURATION_NANOSECONDS: {
        
//      } break;
      case cudf::type_id::DICTIONARY32: {
        
      } break;
      case cudf::type_id::STRING: {
        cudf_cols[col] = build_str_cudf_col(total_rows, host_cols[col], data_size, valids);
      } break;
      case cudf::type_id::LIST: {
        
      } break;
      case cudf::type_id::DECIMAL32: {
        
      } break;
      case cudf::type_id::DECIMAL64: {
        
      } break;
      case cudf::type_id::STRUCT: {
        
      } break;
    }
  }

  ret.tbl = std::make_unique<cudf::table>(std::move(cudf_cols));

  for (int col = 0; col < host_cols.size(); ++col) {
    free(host_cols[col]);
  }

  return ret;
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
  // DEBUG
  //std::cout << "PARSING BATCH: " << handle.sql_handle.row_count << "\n";
  auto res = handle.sql_handle.mysql_resultset;
	if(res == nullptr) {
		return schema.makeEmptyBlazingTable(column_indices);
	}

	if(column_indices.size() > 0) {
		std::vector<std::string> col_names(column_indices.size());

		for(size_t column_i = 0; column_i < column_indices.size(); column_i++) {
			col_names[column_i] = schema.get_name(column_indices[column_i]);
		}

		auto result = read_mysql(res, column_indices, schema.get_dtypes(), handle.sql_handle.column_bytes, handle.sql_handle.row_count);
    result.metadata.column_names = col_names;

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
		cudf::type_id type = parse_mysql_column_type(handle.sql_handle.column_types.at(i));
		size_t file_index = i;
		bool is_in_file = true;
		std::string name = handle.sql_handle.column_names.at(i);
		schema.add_column(name, type, file_index, is_in_file);
	}
}

// TODO percy
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
  return nullptr;
}

} /* namespace io */
} /* namespace ral */
