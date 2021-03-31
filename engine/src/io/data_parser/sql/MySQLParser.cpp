/*
 * Copyright 2021 BlazingDB, Inc.
 *     Copyright 2021 Percy Camilo Triveño Aucahuasi <percy@blazingdb.com>
 */

#include "MySQLParser.h"
#include "sqlcommon.h"

#include <netinet/in.h>

#include <numeric>
#include <arrow/io/file.h>

#include "ExceptionHandling/BlazingThread.h"
#include "Util/StringUtil.h"

#include <mysql/jdbc.h>

namespace ral {
namespace io {

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
      StringUtil::beginsWith(t, "BOOLEAN") || (t == "TINYINT(1)")) return cudf::type_id::BOOL8;
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
  // decimal TODO percy william c.cordova
  if (StringUtil::beginsWith(t, "DECIMAL")) return cudf::type_id::FLOAT64;

  // TODO percy ...
}

std::vector<cudf::type_id> parse_mysql_column_types(const std::vector<std::string> types) {
  std::vector<cudf::type_id> ret;
  for (auto t : types) {
    ret.push_back(parse_mysql_column_type(t));
  }
  return ret;
}

cudf::io::table_with_metadata read_mysql(std::shared_ptr<sql::ResultSet> res,
                                         const std::vector<int> &column_indices,
                                         const std::vector<cudf::type_id> &cudf_types,
                                         size_t total_rows)
{
  cudf::io::table_with_metadata ret;
  std::vector<void*> host_cols(column_indices.size());
  auto const num_words = cudf::bitmask_allocation_size_bytes(total_rows)/sizeof(cudf::bitmask_type);
  auto null_masks = std::vector<std::vector<cudf::bitmask_type>>(host_cols.size());

  for (int col = 0; col < host_cols.size(); ++col) {
    null_masks[col].resize(num_words, 0);
    size_t projection = column_indices[col];
    cudf::type_id cudf_type_id = cudf_types[projection];
    switch (cudf_type_id) {
      case cudf::type_id::EMPTY: {} break;
      case cudf::type_id::INT8: {
        std::vector<int8_t> *tmp = new std::vector<int8_t>();
        tmp->resize(total_rows);
        host_cols[col] = tmp;
      } break;
      case cudf::type_id::INT16: {
        std::vector<int16_t> *tmp = new std::vector<int16_t>();
        tmp->resize(total_rows);
        host_cols[col] = tmp;
      } break;
      case cudf::type_id::INT32: {
        std::vector<int32_t> *tmp = new std::vector<int32_t>();
        tmp->resize(total_rows);
        host_cols[col] = tmp;
      } break;
      case cudf::type_id::INT64: {
        std::vector<int64_t> *tmp = new std::vector<int64_t>();
        tmp->resize(total_rows);
        host_cols[col] = tmp;
      } break;
      case cudf::type_id::UINT8: {
        std::vector<uint8_t> *tmp = new std::vector<uint8_t>();
        tmp->resize(total_rows);
        host_cols[col] = tmp;
      } break;
      case cudf::type_id::UINT16: {
        std::vector<uint16_t> *tmp = new std::vector<uint16_t>();
        tmp->resize(total_rows);
        host_cols[col] = tmp;
      } break;
      case cudf::type_id::UINT32: {
        std::vector<uint32_t> *tmp = new std::vector<uint32_t>();
        tmp->resize(total_rows);
        host_cols[col] = tmp;
      } break;
      case cudf::type_id::UINT64: {
        std::vector<uint64_t> *tmp = new std::vector<uint64_t>();
        tmp->resize(total_rows);
        host_cols[col] = tmp;
      } break;
      case cudf::type_id::FLOAT32: {
        std::vector<float> *tmp = new std::vector<float>();
        tmp->resize(total_rows);
        host_cols[col] = tmp;
      } break;
      case cudf::type_id::FLOAT64: {
        std::vector<double> *tmp = new std::vector<double>();
        tmp->resize(total_rows);
        host_cols[col] = tmp;
      } break;
      case cudf::type_id::BOOL8: {
        std::vector<int8_t> *tmp = new std::vector<int8_t>();
        tmp->resize(total_rows);
        host_cols[col] = tmp;
      } break;
      case cudf::type_id::TIMESTAMP_DAYS:
      case cudf::type_id::TIMESTAMP_SECONDS:
      case cudf::type_id::TIMESTAMP_MILLISECONDS:
      case cudf::type_id::TIMESTAMP_MICROSECONDS:
      case cudf::type_id::TIMESTAMP_NANOSECONDS:
      // TODO percy it seems we don't support this yet
      // case cudf::type_id::DURATION_DAYS: {} break;
      // case cudf::type_id::DURATION_SECONDS: {} break;
      // case cudf::type_id::DURATION_MILLISECONDS: {} break;
      // case cudf::type_id::DURATION_MICROSECONDS: {} break;
      // case cudf::type_id::DURATION_NANOSECONDS: {} break;
      // case cudf::type_id::DICTIONARY32: {} break;
      case cudf::type_id::STRING: {
        auto *tmp = new cudf_string_col();
        tmp->offsets.resize(1, 0);
        host_cols[col] = tmp;
      } break;
      // TODO percy
      case cudf::type_id::LIST: {} break;
      case cudf::type_id::DECIMAL32: {} break;
      case cudf::type_id::DECIMAL64: {} break;
      case cudf::type_id::STRUCT: {} break;
      case cudf::type_id::NUM_TYPE_IDS: {} break;
    }
  }

  int row = 0;
  while (res->next()) {
    for (int col = 0; col < column_indices.size(); ++col) {
      int mysql_col = col + 1; // mysql jdbc getString starts counting from 1 (not from 0)
      size_t projection = column_indices[col];
      uint8_t valid = 1; // 1: not null 0: null
      cudf::type_id cudf_type_id = cudf_types[projection];
      switch (cudf_type_id) {
        case cudf::type_id::EMPTY: {} break;
        case cudf::type_id::INT8: {
          int32_t raw_data = res->getInt(mysql_col);
          if (res->wasNull()) {
            valid = 0;
          } else {
            std::vector<int8_t> *v = (std::vector<int8_t>*)host_cols[col];
            int8_t real_data = static_cast<int8_t>(raw_data);
            (*v)[row] = real_data;
          }
        } break;
        case cudf::type_id::INT16: {
          int32_t raw_data = res->getInt(mysql_col);
          if (res->wasNull()) {
            valid = 0;
          } else {
            std::vector<int16_t> *v = (std::vector<int16_t>*)host_cols[col];
            int16_t real_data = static_cast<int16_t>(raw_data);
            (*v)[row] = real_data;
          }
        } break;
        case cudf::type_id::INT32: {
          int32_t raw_data = res->getInt(mysql_col);
          if (res->wasNull()) {
            valid = 0;
          } else {
            std::vector<int32_t> *v = (std::vector<int32_t>*)host_cols[col];
            (*v)[row] = raw_data;
          }
        } break;
        case cudf::type_id::INT64: {
          int64_t raw_data = res->getInt64(mysql_col);
          if (res->wasNull()) {
            valid = 0;
          } else {
            std::vector<int64_t> *v = (std::vector<int64_t>*)host_cols[col];
            (*v)[row] = raw_data;
          }
        } break;
        case cudf::type_id::UINT8: {
          uint32_t raw_data = res->getUInt(mysql_col);
          if (res->wasNull()) {
            valid = 0;
          } else {
            std::vector<uint8_t> *v = (std::vector<uint8_t>*)host_cols[col];
            uint8_t real_data = static_cast<uint8_t>(raw_data);
            (*v)[row] = real_data;
          }
        } break;
        case cudf::type_id::UINT16: {
          uint32_t raw_data = res->getUInt(mysql_col);
          if (res->wasNull()) {
            valid = 0;
          } else {
            std::vector<uint16_t> *v = (std::vector<uint16_t>*)host_cols[col];
            uint16_t real_data = static_cast<uint16_t>(raw_data);
            (*v)[row] = real_data;
          }
        } break;
        case cudf::type_id::UINT32: {
          uint32_t raw_data = res->getUInt(mysql_col);
          if (res->wasNull()) {
            valid = 0;
          } else {
            std::vector<uint32_t> *v = (std::vector<uint32_t>*)host_cols[col];
            (*v)[row] = raw_data;
          }
        } break;
        case cudf::type_id::UINT64: {
          uint64_t raw_data = res->getUInt64(mysql_col);
          if (res->wasNull()) {
            valid = 0;
          } else {
            std::vector<uint64_t> *v = (std::vector<uint64_t>*)host_cols[col];
            (*v)[row] = raw_data;
          }
        } break;
        case cudf::type_id::FLOAT32: {
          long double raw_data = res->getDouble(mysql_col);
          if (res->wasNull()) {
            valid = 0;
          } else {
            std::vector<float> *v = (std::vector<float>*)host_cols[col];
            float real_data = static_cast<float>(raw_data);
            (*v)[row] = real_data;
          }
        } break;
        case cudf::type_id::FLOAT64: {
          long double raw_data = res->getDouble(mysql_col);
          if (res->wasNull()) {
            valid = 0;
          } else {
            std::vector<double> *v = (std::vector<double>*)host_cols[col];
            double real_data = static_cast<double>(raw_data);
            (*v)[row] = real_data;
          }
        } break;
        case cudf::type_id::BOOL8: {
          bool raw_data = res->getBoolean(mysql_col);
          if (res->wasNull()) {
            valid = 0;
          } else {
            std::vector<int8_t> *v = (std::vector<int8_t>*)host_cols[col];
            int8_t real_data = static_cast<int8_t>(raw_data);
            (*v)[row] = real_data;
          }
        } break;
        case cudf::type_id::TIMESTAMP_DAYS:
        case cudf::type_id::TIMESTAMP_SECONDS:
        case cudf::type_id::TIMESTAMP_MILLISECONDS:
        case cudf::type_id::TIMESTAMP_MICROSECONDS:
        case cudf::type_id::TIMESTAMP_NANOSECONDS:
        // TODO percy it seems we don't support this yet
        // case cudf::type_id::DURATION_DAYS: {} break;
        // case cudf::type_id::DURATION_SECONDS: {} break;
        // case cudf::type_id::DURATION_MILLISECONDS: {} break;
        // case cudf::type_id::DURATION_MICROSECONDS: {} break;
        // case cudf::type_id::DURATION_NANOSECONDS: {} break;
        // case cudf::type_id::DICTIONARY32: {} break;
        case cudf::type_id::STRING: {
          std::string real_data = res->getString(mysql_col).asStdString();
          cudf_string_col *v = (cudf_string_col*)host_cols[col];
          if (res->wasNull()) {
            valid = 0;
          } else {
            v->chars.insert(v->chars.end(), std::cbegin(real_data), std::cend(real_data));
          }
          auto len = valid? real_data.length() : 0;
          v->offsets.push_back(v->offsets.back() + len);
        } break;
        // TODO percy
        case cudf::type_id::LIST: {} break;
        case cudf::type_id::DECIMAL32: {} break;
        case cudf::type_id::DECIMAL64: {} break;
        case cudf::type_id::STRUCT: {} break;
        case cudf::type_id::NUM_TYPE_IDS: {} break;
      }

      if (valid) {
        cudf::set_bit_unsafe(null_masks[col].data(), row);
      }
    }
    ++row;
  }

  std::vector<std::unique_ptr<cudf::column>> cudf_cols(host_cols.size());

  for (int col = 0; col < column_indices.size(); ++col) {
    size_t projection = column_indices[col];
    cudf::type_id cudf_type_id = cudf_types[projection];
    switch (cudf_type_id) {
      case cudf::type_id::EMPTY: {} break;
      case cudf::type_id::INT8: {
        std::vector<int8_t> *v = (std::vector<int8_t>*)host_cols[col];
        cudf_cols[col] = build_fixed_width_cudf_col<int8_t>(total_rows, v, null_masks[col], cudf_type_id);
      } break;
      case cudf::type_id::INT16: {
        std::vector<int16_t> *v = (std::vector<int16_t>*)host_cols[col];
        cudf_cols[col] = build_fixed_width_cudf_col<int16_t>(total_rows, v, null_masks[col], cudf_type_id);
      } break;
      case cudf::type_id::INT32: {
        std::vector<int32_t> *v = (std::vector<int32_t>*)host_cols[col];
        cudf_cols[col] = build_fixed_width_cudf_col<int32_t>(total_rows, v, null_masks[col], cudf_type_id);
      } break;
      case cudf::type_id::INT64: {
        std::vector<int64_t> *v = (std::vector<int64_t>*)host_cols[col];
        cudf_cols[col] = build_fixed_width_cudf_col<int64_t>(total_rows, v, null_masks[col], cudf_type_id);
      } break;
      case cudf::type_id::UINT8: {
        std::vector<uint8_t> *v = (std::vector<uint8_t>*)host_cols[col];
        cudf_cols[col] = build_fixed_width_cudf_col<uint8_t>(total_rows, v, null_masks[col], cudf_type_id);
      } break;
      case cudf::type_id::UINT16: {
        std::vector<uint16_t> *v = (std::vector<uint16_t>*)host_cols[col];
        cudf_cols[col] = build_fixed_width_cudf_col<uint16_t>(total_rows, v, null_masks[col], cudf_type_id);
      } break;
      case cudf::type_id::UINT32: {
        std::vector<uint32_t> *v = (std::vector<uint32_t>*)host_cols[col];
        cudf_cols[col] = build_fixed_width_cudf_col<uint32_t>(total_rows, v, null_masks[col], cudf_type_id);
      } break;
      case cudf::type_id::UINT64: {
        std::vector<uint64_t> *v = (std::vector<uint64_t>*)host_cols[col];
        cudf_cols[col] = build_fixed_width_cudf_col<uint64_t>(total_rows, v, null_masks[col], cudf_type_id);
      } break;
      case cudf::type_id::FLOAT32: {
        std::vector<float> *v = (std::vector<float>*)host_cols[col];
        cudf_cols[col] = build_fixed_width_cudf_col<float>(total_rows, v, null_masks[col], cudf_type_id);
      } break;
      case cudf::type_id::FLOAT64: {
        std::vector<double> *v = (std::vector<double>*)host_cols[col];
        cudf_cols[col] = build_fixed_width_cudf_col<double>(total_rows, v, null_masks[col], cudf_type_id);
      } break;
      case cudf::type_id::BOOL8: {
        std::vector<uint8_t> *v = (std::vector<uint8_t>*)host_cols[col];
        cudf_cols[col] = build_fixed_width_cudf_col<uint8_t>(total_rows, v, null_masks[col], cudf_type_id);
      } break;
      case cudf::type_id::TIMESTAMP_DAYS: {
        cudf_string_col *v = (cudf_string_col*)host_cols[col];
        auto str_col = build_str_cudf_col(v, null_masks[col]);
        auto vals_col = cudf::strings::to_timestamps(str_col->view(), cudf::data_type{cudf::type_id::TIMESTAMP_DAYS}, "%Y-%m-%d");
        cudf_cols[col] = std::move(vals_col);
      } break;
      case cudf::type_id::TIMESTAMP_SECONDS: {
        cudf_string_col *v = (cudf_string_col*)host_cols[col];
        auto str_col = build_str_cudf_col(v, null_masks[col]);
        auto vals_col = cudf::strings::to_timestamps(str_col->view(), cudf::data_type{cudf::type_id::TIMESTAMP_SECONDS}, "%H:%M:%S");
        cudf_cols[col] = std::move(vals_col);
      } break;
      case cudf::type_id::TIMESTAMP_MILLISECONDS: {
        cudf_string_col *v = (cudf_string_col*)host_cols[col];
        auto str_col = build_str_cudf_col(v, null_masks[col]);
        auto vals_col = cudf::strings::to_timestamps(str_col->view(), cudf::data_type{cudf::type_id::TIMESTAMP_MILLISECONDS}, "%Y-%m-%d %H:%M:%S");
        cudf_cols[col] = std::move(vals_col);
      } break;
      case cudf::type_id::TIMESTAMP_MICROSECONDS: {} break;
      case cudf::type_id::TIMESTAMP_NANOSECONDS: {} break;
      // TODO percy it seems we don't support this yet
      // case cudf::type_id::DURATION_DAYS: {} break;
      // case cudf::type_id::DURATION_SECONDS: {} break;
      // case cudf::type_id::DURATION_MILLISECONDS: {} break;
      // case cudf::type_id::DURATION_MICROSECONDS: {} break;
      // case cudf::type_id::DURATION_NANOSECONDS: {} break;
      // case cudf::type_id::DICTIONARY32: {} break;
      case cudf::type_id::DICTIONARY32: {} break;
      case cudf::type_id::STRING: {
        cudf_string_col *v = (cudf_string_col*)host_cols[col];
        cudf_cols[col] = build_str_cudf_col(v, null_masks[col]);
      } break;
      case cudf::type_id::LIST: {} break;
      case cudf::type_id::DECIMAL32: {} break;
      case cudf::type_id::DECIMAL64: {} break;
      case cudf::type_id::STRUCT: {} break;
      case cudf::type_id::NUM_TYPE_IDS: {} break;
    }
  }

  ret.tbl = std::make_unique<cudf::table>(std::move(cudf_cols));

  for (int col = 0; col < host_cols.size(); ++col) {
    size_t projection = column_indices[col];
    cudf::type_id cudf_type_id = cudf_types[projection];
    switch (cudf_type_id) {
      case cudf::type_id::EMPTY: {} break;
      case cudf::type_id::INT8: {
        std::vector<int8_t> *tmp = (std::vector<int8_t>*)host_cols[col];
        delete(tmp);
      } break;
      case cudf::type_id::INT16: {
        std::vector<int16_t> *tmp = (std::vector<int16_t>*)host_cols[col];
        delete(tmp);
      } break;
      case cudf::type_id::INT32: {
        std::vector<int32_t> *tmp = (std::vector<int32_t>*)host_cols[col];
        delete(tmp);
      } break;
      case cudf::type_id::INT64: {
        std::vector<int64_t> *tmp = (std::vector<int64_t>*)host_cols[col];
        delete(tmp);
      } break;
      case cudf::type_id::UINT8: {
        std::vector<uint8_t> *tmp = (std::vector<uint8_t>*)host_cols[col];
        delete(tmp);
      } break;
      case cudf::type_id::UINT16: {
        std::vector<uint16_t> *tmp = (std::vector<uint16_t>*)host_cols[col];
        delete(tmp);
      } break;
      case cudf::type_id::UINT32: {
        std::vector<uint32_t> *tmp = (std::vector<uint32_t>*)host_cols[col];
        delete(tmp);
      } break;
      case cudf::type_id::UINT64: {
        std::vector<uint64_t> *tmp = (std::vector<uint64_t>*)host_cols[col];
        delete(tmp);
      } break;
      case cudf::type_id::FLOAT32: {
        std::vector<float> *tmp = (std::vector<float>*)host_cols[col];
        delete(tmp);
      } break;
      case cudf::type_id::FLOAT64: {
        std::vector<double> *tmp = (std::vector<double>*)host_cols[col];
        delete(tmp);
      } break;
      case cudf::type_id::BOOL8: {
        std::vector<int8_t> *tmp = (std::vector<int8_t>*)host_cols[col];
        delete(tmp);
      } break;
      case cudf::type_id::TIMESTAMP_DAYS:
      case cudf::type_id::TIMESTAMP_SECONDS:
      case cudf::type_id::TIMESTAMP_MILLISECONDS:
      case cudf::type_id::TIMESTAMP_MICROSECONDS:
      case cudf::type_id::TIMESTAMP_NANOSECONDS:
      // TODO percy it seems we don't support this yet
      // case cudf::type_id::DURATION_DAYS: {} break;
      // case cudf::type_id::DURATION_SECONDS: {} break;
      // case cudf::type_id::DURATION_MILLISECONDS: {} break;
      // case cudf::type_id::DURATION_MICROSECONDS: {} break;
      // case cudf::type_id::DURATION_NANOSECONDS: {} break;
      // case cudf::type_id::DICTIONARY32: {} break;
      case cudf::type_id::STRING: {
        cudf_string_col *tmp = (cudf_string_col*)host_cols[col];
        delete(tmp);
      } break;
      // TODO percy
      case cudf::type_id::LIST: {} break;
      case cudf::type_id::DECIMAL32: {} break;
      case cudf::type_id::DECIMAL64: {} break;
      case cudf::type_id::STRUCT: {} break;
      case cudf::type_id::NUM_TYPE_IDS: {} break;
    }
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

		auto result = read_mysql(res, column_indices, schema.get_dtypes(), handle.sql_handle.row_count);
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
