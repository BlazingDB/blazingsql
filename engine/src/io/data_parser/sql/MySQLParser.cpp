/*
 * Copyright 2021 Percy Camilo Triveño Aucahuasi <percy.camilo.ta@gmail.com>
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

mysql_parser::mysql_parser(): abstractsql_parser(DataType::MYSQL) {
}

mysql_parser::~mysql_parser() {
}

void mysql_parser::read_sql_loop(void *src,
    const std::vector<cudf::type_id> &cudf_types,
    const std::vector<int> &column_indices,
    std::vector<void*> &host_cols,
    std::vector<std::vector<cudf::bitmask_type>> &null_masks)
{
  int row = 0;
  auto res = (sql::ResultSet*)src;
  while (res->next()) {
    this->parse_sql(src, column_indices, cudf_types, row, host_cols, null_masks);
    ++row;
  }
}

cudf::type_id mysql_parser::get_cudf_type_id(const std::string &sql_column_type) {
  return parse_mysql_column_type(sql_column_type);
}

// returns 0:false if the value was null ... otherwise returns 1:true
uint8_t mysql_parser::parse_cudf_int8(void *src, size_t col, size_t row, std::vector<int8_t> *v)
{
  auto res = (sql::ResultSet*)src;
  ++col; // mysql count columns starting from 1
  int32_t raw_data = res->getInt(col);
  if (res->wasNull()) return 0;
  int8_t real_data = static_cast<int8_t>(raw_data);
  (*v)[row] = real_data;
  return 1;
}

uint8_t mysql_parser::parse_cudf_int16(void *src, size_t col, size_t row, std::vector<int16_t> *v)
{
  auto res = (sql::ResultSet*)src;
  ++col; // mysql count columns starting from 1
  int32_t raw_data = res->getInt(col);
  if (res->wasNull()) return 0;
  int16_t real_data = static_cast<int16_t>(raw_data);
  (*v)[row] = real_data;
  return 1;
}

uint8_t mysql_parser::parse_cudf_int32(void *src, size_t col, size_t row, std::vector<int32_t> *v)
{
  auto res = (sql::ResultSet*)src;
  ++col; // mysql count columns starting from 1
  int32_t raw_data = res->getInt(col);
  if (res->wasNull()) return 0;
  (*v)[row] = raw_data;
  return 1;
}

uint8_t mysql_parser::parse_cudf_int64(void *src, size_t col, size_t row, std::vector<int64_t> *v)
{
  auto res = (sql::ResultSet*)src;
  ++col; // mysql count columns starting from 1
  int64_t raw_data = res->getInt64(col);
  if (res->wasNull()) return 0;
  (*v)[row] = raw_data;
  return 1;
}

uint8_t mysql_parser::parse_cudf_uint8(void *src, size_t col, size_t row, std::vector<uint8_t> *v)
{
  auto res = (sql::ResultSet*)src;
  ++col; // mysql count columns starting from 1
  uint32_t raw_data = res->getUInt(col);
  if (res->wasNull()) return 0;
  uint8_t real_data = static_cast<uint8_t>(raw_data);
  (*v)[row] = real_data;
  return 1;
}

uint8_t mysql_parser::parse_cudf_uint16(void *src, size_t col, size_t row, std::vector<uint16_t> *v)
{
  auto res = (sql::ResultSet*)src;
  ++col; // mysql count columns starting from 1
  uint32_t raw_data = res->getUInt(col);
  if (res->wasNull()) return 0;
  uint16_t real_data = static_cast<uint16_t>(raw_data);
  (*v)[row] = real_data;
  return 1;
}

uint8_t mysql_parser::parse_cudf_uint32(void *src, size_t col, size_t row, std::vector<uint32_t> *v )
{
  auto res = (sql::ResultSet*)src;
  ++col; // mysql count columns starting from 1
  uint32_t raw_data = res->getUInt(col);
  if (res->wasNull()) return 0;
  (*v)[row] = raw_data;
  return 1;
}

uint8_t mysql_parser::parse_cudf_uint64(void *src, size_t col, size_t row, std::vector<uint64_t> *v)
{
  auto res = (sql::ResultSet*)src;
  ++col; // mysql count columns starting from 1
  uint64_t raw_data = res->getUInt64(col);
  if (res->wasNull()) return 0;
  (*v)[row] = raw_data;
  return 1;
}

uint8_t mysql_parser::parse_cudf_float32(void *src, size_t col, size_t row, std::vector<float> *v)
{
  auto res = (sql::ResultSet*)src;
  ++col; // mysql count columns starting from 1
  long double raw_data = res->getDouble(col);
  if (res->wasNull()) return 0;
  float real_data = static_cast<float>(raw_data);
  (*v)[row] = real_data;
  return 1;
}

uint8_t mysql_parser::parse_cudf_float64(void *src, size_t col, size_t row, std::vector<double> *v)
{
  auto res = (sql::ResultSet*)src;
  ++col; // mysql count columns starting from 1
  long double raw_data = res->getDouble(col);
  if (res->wasNull()) return 0;
  double real_data = static_cast<double>(raw_data);
  (*v)[row] = real_data;
  return 1;
}

uint8_t mysql_parser::parse_cudf_bool8(void *src, size_t col, size_t row, std::vector<int8_t> *v)
{
  auto res = (sql::ResultSet*)src;
  ++col; // mysql count columns starting from 1
  bool raw_data = res->getBoolean(col);
  if (res->wasNull()) return 0;
  int8_t real_data = static_cast<int8_t>(raw_data);
  (*v)[row] = real_data;
  return 1;
}

uint8_t mysql_parser::parse_cudf_timestamp_days(void *src, size_t col, size_t row, cudf_string_col *v)
{
  return this->parse_cudf_string(src, col, row, v);
}

uint8_t mysql_parser::parse_cudf_timestamp_seconds(void *src, size_t col, size_t row, cudf_string_col *v)
{
  return this->parse_cudf_string(src, col, row, v);
}

uint8_t mysql_parser::parse_cudf_timestamp_milliseconds(void *src, size_t col, size_t row, cudf_string_col *v)
{
  return this->parse_cudf_string(src, col, row, v);
}

uint8_t mysql_parser::parse_cudf_timestamp_microseconds(void *src, size_t col, size_t row, cudf_string_col *v)
{
  return this->parse_cudf_string(src, col, row, v);
}

uint8_t mysql_parser::parse_cudf_timestamp_nanoseconds(void *src, size_t col, size_t row, cudf_string_col *v)
{
  return this->parse_cudf_string(src, col, row, v);
}

// todo percy it seems we don't support this yet
// case cudf::type_id::duration_days: {} break;
// case cudf::type_id::duration_seconds: {} break;
// case cudf::type_id::duration_milliseconds: {} break;
// case cudf::type_id::duration_microseconds: {} break;
// case cudf::type_id::duration_nanoseconds: {} break;
// case cudf::type_id::dictionary32: {} break;

uint8_t mysql_parser::parse_cudf_string(void *src, size_t col, size_t /*row*/, cudf_string_col *v)
{
  auto res = (sql::ResultSet*)src;
  ++col; // mysql count columns starting from 1
  std::string real_data = res->getString(col).asStdString();
  uint8_t valid = 1;
  if (res->wasNull()) {
    valid = 0;
  } else {
    v->chars.insert(v->chars.end(), std::cbegin(real_data), std::cend(real_data));
  }
  auto len = valid? real_data.length() : 0;
  v->offsets.push_back(v->offsets.back() + len);
  return valid;
}

//virtual bool parse_cudf_list(void *src, size_t col, size_t row){}
//virtual bool parse_cudf_decimal32(void *src, size_t col, size_t row){}
//virtual bool parse_cudf_decimal64(void *src, size_t col, size_t row){}
//virtual bool parse_cudf_struct(void *src, size_t col, size_t row){}

} /* namespace io */
} /* namespace ral */
