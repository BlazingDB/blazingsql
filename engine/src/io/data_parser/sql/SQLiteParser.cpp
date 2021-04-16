/*
 * Copyright 2021 Percy Camilo Triveño Aucahuasi <percy.camilo.ta@gmail.com>
 * Copyright 2021 Cristhian Alberto Gonzales Castillo
 */

#include <array>
#include <numeric>

#include "SQLiteParser.h"
#include "sqlcommon.h"

#include "utilities/CommonOperations.h"

#include "ExceptionHandling/BlazingThread.h"
#include <arrow/io/file.h>
#include <sqlite3.h>

#include <parquet/column_writer.h>
#include <parquet/file_writer.h>

#include "Util/StringUtil.h"

namespace ral {
namespace io {

namespace cudf_io = cudf::io;

// TODO percy this is too naive ... improve this later
// TEXT types:
// - CHARACTER(20)
// - VARCHAR(255)
// - VARYING CHARACTER(255)
// - NCHAR(55)
// - NATIVE CHARACTER(70)
// - NVARCHAR(100)
// - TEXT
// - CLOB
static const std::array<const char *, 10> mysql_string_types_hints = {
    "character",
    "varchar",
    "char",
    "varying character",
    "nchar",
    "native character",
    "nvarchar",
    "text",
    "clob",
    "string"  // TODO percy ???
};

bool sqlite_is_cudf_string(const std::string & t) {
  for (auto hint : mysql_string_types_hints) {
    if (StringUtil::beginsWith(t, std::string{hint})) return true;
  }
  return false;
}

cudf::type_id parse_sqlite_column_type(std::string t) {
  std::transform(
      t.cbegin(), t.cend(), t.begin(), [](const std::string::value_type c) {
        return std::tolower(c);
      });
  if (sqlite_is_cudf_string(t)) return cudf::type_id::STRING;
  if (t == "tinyint") { return cudf::type_id::INT8; }
  if (t == "smallint") { return cudf::type_id::INT8; }
  if (t == "mediumint") { return cudf::type_id::INT16; }
  if (t == "int") { return cudf::type_id::INT32; }
  if (t == "integer") { return cudf::type_id::INT32; }
  if (t == "bigint") { return cudf::type_id::INT64; }
  if (t == "unsigned big int") { return cudf::type_id::UINT64; }
  if (t == "int2") { return cudf::type_id::INT16; }
  if (t == "int8") { return cudf::type_id::INT64; }
  if (t == "real") { return cudf::type_id::FLOAT32; }
  if (t == "double") { return cudf::type_id::FLOAT64; }
  if (t == "double precision") { return cudf::type_id::FLOAT64; }
  if (t == "float") { return cudf::type_id::FLOAT32; }
  if (t == "decimal") { return cudf::type_id::FLOAT64; }
  if (t == "boolean") { return cudf::type_id::UINT8; }
  if (t == "date") { return cudf::type_id::TIMESTAMP_MILLISECONDS; }
  if (t == "datetime") { return cudf::type_id::TIMESTAMP_MILLISECONDS; }
}

sqlite_parser::sqlite_parser() : abstractsql_parser{DataType::SQLITE} {}

sqlite_parser::~sqlite_parser() = default;

void sqlite_parser::read_sql_loop(void * src,
    const std::vector<cudf::type_id> & cudf_types,
    const std::vector<int> & column_indices,
    std::vector<void *> & host_cols,
    std::vector<std::vector<cudf::bitmask_type>> & null_masks) {
  int rowCounter = 0;
  sqlite3_stmt * stmt = reinterpret_cast<sqlite3_stmt *>(src);
  while (sqlite3_step(stmt) == SQLITE_ROW) {
    parse_sql(
        src, column_indices, cudf_types, rowCounter, host_cols, null_masks);
    ++rowCounter;
  }
}

cudf::type_id sqlite_parser::get_cudf_type_id(
    const std::string & sql_column_type) {
  return parse_sqlite_column_type(sql_column_type);
}

// To know about postgresql data type details
// see https://www.postgresql.org/docs/current/datatype.html

std::uint8_t sqlite_parser::parse_cudf_int8(void * src,
    std::size_t col,
    std::size_t row,
    std::vector<std::int8_t> * v) {
  sqlite3_stmt * stmt = reinterpret_cast<sqlite3_stmt *>(src);
  if (sqlite3_column_type(stmt, col) == SQLITE_NULL) { return 0; }
  const std::int8_t value =
      static_cast<std::int8_t>(sqlite3_column_int(stmt, col));
  v->at(row) = value;
  return 1;
}

std::uint8_t sqlite_parser::parse_cudf_int16(void * src,
    std::size_t col,
    std::size_t row,
    std::vector<std::int16_t> * v) {
  sqlite3_stmt * stmt = reinterpret_cast<sqlite3_stmt *>(src);
  if (sqlite3_column_type(stmt, col) == SQLITE_NULL) { return 0; }
  const std::int16_t value =
      static_cast<std::int16_t>(sqlite3_column_int(stmt, col));
  v->at(row) = value;
  return 1;
}

std::uint8_t sqlite_parser::parse_cudf_int32(void * src,
    std::size_t col,
    std::size_t row,
    std::vector<std::int32_t> * v) {
  sqlite3_stmt * stmt = reinterpret_cast<sqlite3_stmt *>(src);
  if (sqlite3_column_type(stmt, col) == SQLITE_NULL) { return 0; }
  const std::int32_t value =
      static_cast<std::int32_t>(sqlite3_column_int(stmt, col));
  v->at(row) = value;
  return 1;
}

std::uint8_t sqlite_parser::parse_cudf_int64(void * src,
    std::size_t col,
    std::size_t row,
    std::vector<std::int64_t> * v) {
  sqlite3_stmt * stmt = reinterpret_cast<sqlite3_stmt *>(src);
  if (sqlite3_column_type(stmt, col) == SQLITE_NULL) { return 0; }
  const std::int64_t value =
      static_cast<std::int64_t>(sqlite3_column_int64(stmt, col));
  v->at(row) = value;
  return 1;
}

std::uint8_t sqlite_parser::parse_cudf_uint8(void * src,
    std::size_t col,
    std::size_t row,
    std::vector<std::uint8_t> * v) {
  sqlite3_stmt * stmt = reinterpret_cast<sqlite3_stmt *>(src);
  if (sqlite3_column_type(stmt, col) == SQLITE_NULL) { return 0; }
  const std::uint8_t value =
      static_cast<std::uint8_t>(sqlite3_column_int(stmt, col));
  v->at(row) = value;
  return 1;
}

std::uint8_t sqlite_parser::parse_cudf_uint16(void * src,
    std::size_t col,
    std::size_t row,
    std::vector<std::uint16_t> * v) {
  sqlite3_stmt * stmt = reinterpret_cast<sqlite3_stmt *>(src);
  if (sqlite3_column_type(stmt, col) == SQLITE_NULL) { return 0; }
  const std::uint16_t value =
      static_cast<std::uint16_t>(sqlite3_column_int(stmt, col));
  v->at(row) = value;
  return 1;
}

std::uint8_t sqlite_parser::parse_cudf_uint32(void * src,
    std::size_t col,
    std::size_t row,
    std::vector<std::uint32_t> * v) {
  sqlite3_stmt * stmt = reinterpret_cast<sqlite3_stmt *>(src);
  if (sqlite3_column_type(stmt, col) == SQLITE_NULL) { return 0; }
  const std::uint32_t value =
      static_cast<std::uint32_t>(sqlite3_column_int(stmt, col));
  v->at(row) = value;
  return 1;
}

std::uint8_t sqlite_parser::parse_cudf_uint64(void * src,
    std::size_t col,
    std::size_t row,
    std::vector<std::uint64_t> * v) {
  sqlite3_stmt * stmt = reinterpret_cast<sqlite3_stmt *>(src);
  if (sqlite3_column_type(stmt, col) == SQLITE_NULL) { return 0; }
  const std::uint64_t value =
      static_cast<std::uint64_t>(sqlite3_column_int64(stmt, col));
  v->at(row) = value;
  return 1;
}

std::uint8_t sqlite_parser::parse_cudf_float32(
    void * src, std::size_t col, std::size_t row, std::vector<float> * v) {
  sqlite3_stmt * stmt = reinterpret_cast<sqlite3_stmt *>(src);
  if (sqlite3_column_type(stmt, col) == SQLITE_NULL) { return 0; }
  const float value = static_cast<float>(sqlite3_column_double(stmt, col));
  v->at(row) = value;
  return 1;
}

std::uint8_t sqlite_parser::parse_cudf_float64(
    void * src, std::size_t col, std::size_t row, std::vector<double> * v) {
  sqlite3_stmt * stmt = reinterpret_cast<sqlite3_stmt *>(src);
  if (sqlite3_column_type(stmt, col) == SQLITE_NULL) { return 0; }
  const double value = sqlite3_column_double(stmt, col);
  v->at(row) = value;
  return 1;
}

std::uint8_t sqlite_parser::parse_cudf_bool8(void * src,
    std::size_t col,
    std::size_t row,
    std::vector<std::int8_t> * v) {
  sqlite3_stmt * stmt = reinterpret_cast<sqlite3_stmt *>(src);
  if (sqlite3_column_type(stmt, col) == SQLITE_NULL) { return 0; }
  const std::int8_t value =
      static_cast<std::int8_t>(sqlite3_column_int(stmt, col));
  v->at(row) = value;
  return 1;
}

std::uint8_t sqlite_parser::parse_cudf_timestamp_days(
    void * src, std::size_t col, std::size_t row, cudf_string_col * v) {
  return parse_cudf_string(src, col, row, v);
}

std::uint8_t sqlite_parser::parse_cudf_timestamp_seconds(
    void * src, std::size_t col, std::size_t row, cudf_string_col * v) {
  return parse_cudf_string(src, col, row, v);
}

std::uint8_t sqlite_parser::parse_cudf_timestamp_milliseconds(
    void * src, std::size_t col, std::size_t row, cudf_string_col * v) {
  return parse_cudf_string(src, col, row, v);
}

std::uint8_t sqlite_parser::parse_cudf_timestamp_microseconds(
    void * src, std::size_t col, std::size_t row, cudf_string_col * v) {
  return parse_cudf_string(src, col, row, v);
}

std::uint8_t sqlite_parser::parse_cudf_timestamp_nanoseconds(
    void * src, std::size_t col, std::size_t row, cudf_string_col * v) {
  return parse_cudf_string(src, col, row, v);
}

std::uint8_t sqlite_parser::parse_cudf_string(
    void * src, std::size_t col, std::size_t row, cudf_string_col * v) {
  sqlite3_stmt * stmt = reinterpret_cast<sqlite3_stmt *>(src);

  std::string column_decltype = sqlite3_column_decltype(stmt, col);

  if (sqlite3_column_type(stmt, col) == SQLITE_NULL) {
    v->offsets.push_back(v->offsets.back());
    return 0;
  } else {
    const unsigned char * text = sqlite3_column_text(stmt, col);
    const std::string value{reinterpret_cast<const char *>(text)};
    v->chars.insert(v->chars.end(), value.cbegin(), value.cend());
    v->offsets.push_back(v->offsets.back() + value.length());
    return 1;
  }
}

} /* namespace io */
} /* namespace ral */
