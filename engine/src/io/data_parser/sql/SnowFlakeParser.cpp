/**
 * Copyright 2021 Cristhian Alberto Gonzales Castillo <gcca.lib@gmail.com>
 */

#include <sstream>

#include "SnowFlakeParser.h"
#include "sqlcommon.h"

#define	BOOL int
#include <sql.h>
#include <sqlext.h>

namespace ral {
namespace io {

static const std::array<const char *, 6> postgresql_string_type_hints =
    {"character", "character varying", "bytea", "text", "anyarray", "name"};

static inline bool postgresql_is_cudf_string(const std::string & hint) {
  const auto * result = std::find_if(
      std::cbegin(postgresql_string_type_hints),
      std::cend(postgresql_string_type_hints),
      [&hint](const char * c_hint) { return !hint.rfind(c_hint, 0); });
  return result != std::cend(postgresql_string_type_hints);
}

static inline void ThrowParseError[[noreturn]](const std::size_t col) {
  std::ostringstream oss;
  oss << "SnowFlake: parsing col " << col << " as int64";
  throw std::runtime_error{oss.str()};
}

snowflake_parser::snowflake_parser()
    : abstractsql_parser{DataType::SNOWFLAKE} {}

snowflake_parser::~snowflake_parser() = default;

void snowflake_parser::read_sql_loop(
    void * src,
    const std::vector<cudf::type_id> & cudf_types,
    const std::vector<int> & column_indices,
    std::vector<void *> & host_cols,
    std::vector<std::vector<cudf::bitmask_type>> & null_masks) {
  SQLHDBC sqlHStmt = *static_cast<SQLHDBC *>(src);
  SQLLEN RowCount = 0;
  SQLRETURN sqlReturn = SQLRowCount(sqlHStmt, &RowCount);
  if (sqlReturn != SQL_SUCCESS) {
    throw std::runtime_error("SnowFlake: getting row count for parsing");
  }
  for (SQLLEN Counter = 0; Counter < RowCount; Counter++) {
    parse_sql(src, column_indices, cudf_types, Counter, host_cols, null_masks);
  }
}

cudf::type_id
snowflake_parser::get_cudf_type_id(const std::string & sql_column_type) {
  if (sql_column_type == "string") { return cudf::type_id::STRING; }
  if (sql_column_type == "date") {
    return cudf::type_id::TIMESTAMP_MILLISECONDS;
  }
  if (sql_column_type == "boolean") { return cudf::type_id::BOOL8; }
  if (sql_column_type == "timestamp") {
    return cudf::type_id::TIMESTAMP_MILLISECONDS;
  }
  if (sql_column_type == "int64") { return cudf::type_id::INT64; }
  if (sql_column_type == "float64") { return cudf::type_id::FLOAT64; }

  throw std::runtime_error{"SnowFlake parser: unsupported cudf type"};
}

std::uint8_t snowflake_parser::parse_cudf_int8(void *,
                                               std::size_t,
                                               std::size_t,
                                               std::vector<std::int8_t> *) {
  return 0;
}
std::uint8_t snowflake_parser::parse_cudf_int16(void *,
                                                std::size_t,
                                                std::size_t,
                                                std::vector<std::int16_t> *) {
  return 0;
}
std::uint8_t snowflake_parser::parse_cudf_int32(void *,
                                                std::size_t,
                                                std::size_t,
                                                std::vector<std::int32_t> *) {
  return 0;
}
std::uint8_t snowflake_parser::parse_cudf_int64(void * src,
                                                std::size_t col,
                                                std::size_t row,
                                                std::vector<std::int64_t> * v) {
  SQLHDBC sqlHStmt = *static_cast<SQLHDBC *>(src);
  SQLLEN indicator;
  std::int64_t value;
  SQLRETURN sqlReturn = SQLGetData(sqlHStmt,
                                   col + 1,
                                   SQL_C_LONG,
                                   static_cast<SQLPOINTER>(&value),
                                   static_cast<SQLLEN>(sizeof(value)),
                                   &indicator);
  if (SQL_SUCCEEDED(sqlReturn)) {
    if (indicator == SQL_NULL_DATA) { return 0; }
    v->at(row) = value;
    return 1;
  }

  ThrowParseError(col);
}

std::uint8_t snowflake_parser::parse_cudf_uint8(void *,
                                                std::size_t,
                                                std::size_t,
                                                std::vector<std::uint8_t> *) {
  return 0;
}
std::uint8_t snowflake_parser::parse_cudf_uint16(void *,
                                                 std::size_t,
                                                 std::size_t,
                                                 std::vector<std::uint16_t> *) {
  return 0;
}
std::uint8_t snowflake_parser::parse_cudf_uint32(void *,
                                                 std::size_t,
                                                 std::size_t,
                                                 std::vector<std::uint32_t> *) {
  return 0;
}
std::uint8_t snowflake_parser::parse_cudf_uint64(void *,
                                                 std::size_t,
                                                 std::size_t,
                                                 std::vector<std::uint64_t> *) {
  return 0;
}
std::uint8_t snowflake_parser::parse_cudf_float32(void *,
                                                  std::size_t,
                                                  std::size_t,
                                                  std::vector<float> *) {
  return 0;
}
std::uint8_t snowflake_parser::parse_cudf_float64(void *,
                                                  std::size_t,
                                                  std::size_t,
                                                  std::vector<double> *) {
  return 0;
}
std::uint8_t snowflake_parser::parse_cudf_bool8(void *,
                                                std::size_t,
                                                std::size_t,
                                                std::vector<std::int8_t> *) {
  return 0;
}
std::uint8_t snowflake_parser::parse_cudf_timestamp_days(void *,
                                                         std::size_t,
                                                         std::size_t,
                                                         cudf_string_col *) {
  return 0;
}
std::uint8_t snowflake_parser::parse_cudf_timestamp_seconds(void *,
                                                            std::size_t,
                                                            std::size_t,
                                                            cudf_string_col *) {
  return 0;
}
std::uint8_t
snowflake_parser::parse_cudf_timestamp_milliseconds(void *,
                                                    std::size_t,
                                                    std::size_t,
                                                    cudf_string_col *) {
  return 0;
}
std::uint8_t
snowflake_parser::parse_cudf_timestamp_microseconds(void *,
                                                    std::size_t,
                                                    std::size_t,
                                                    cudf_string_col *) {
  return 0;
}
std::uint8_t
snowflake_parser::parse_cudf_timestamp_nanoseconds(void *,
                                                   std::size_t,
                                                   std::size_t,
                                                   cudf_string_col *) {
  return 0;
}
std::uint8_t snowflake_parser::parse_cudf_string(void *,
                                                 std::size_t,
                                                 std::size_t,
                                                 cudf_string_col *) {
  return 0;
}


}  // namespace io
}  // namespace ral
