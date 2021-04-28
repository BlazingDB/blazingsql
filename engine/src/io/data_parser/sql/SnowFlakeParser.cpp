/**
 * Copyright 2021 Cristhian Alberto Gonzales Castillo <gcca.lib@gmail.com>
 */

#include <sstream>

#include "SnowFlakeParser.h"
#include "sqlcommon.h"

#define BOOL int
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

static inline void ThrowParseError[[noreturn]](const std::size_t col,
                                               const std::size_t row) {
  std::ostringstream oss;
  oss << "SnowFlake: parsing col " << col << " and row " << row;
  throw std::runtime_error{oss.str()};
}

namespace {

template <class T>
class Traits {
public:
  template <class U = T>
  static constexpr typename std::enable_if_t<std::is_integral_v<U>, T>
  strtot(const char * s) {
    char * end = nullptr;
    return static_cast<T>(std::strtoll(s, &end, 10));
  }

  template <class U = T>
  static constexpr typename std::enable_if_t<std::is_floating_point_v<U>, T>
  strtot(const char * s) {
    char * end = nullptr;
    return static_cast<T>(std::strtod(s, &end));
  }
};

}  // namespace

template <class T>
static inline std::uint8_t ParseCudfType(void * src,
                                         const std::size_t col,
                                         const std::size_t row,
                                         std::vector<std::int64_t> * v) {
  SQLHDBC sqlHStmt = *static_cast<SQLHDBC *>(src);
  SQLLEN indicator;
  SQLCHAR buffer[512];
  SQLRETURN sqlReturn = SQLGetData(sqlHStmt,
                                   col + 1,
                                   SQL_C_CHAR,
                                   static_cast<SQLPOINTER>(&buffer),
                                   static_cast<SQLLEN>(sizeof(buffer)),
                                   &indicator);
  if (SQL_SUCCEEDED(sqlReturn)) {
    if (indicator == SQL_NULL_DATA) { return 0; }
    const T value = Traits<T>::strtot(reinterpret_cast<const char *>(buffer));
    v->at(row) = value;
    return 1;
  }
  ThrowParseError(col, row);
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
  SQLRETURN sqlReturn;

  std::size_t counter = 0;
  while (SQL_SUCCEEDED(sqlReturn = SQLFetch(sqlHStmt))) {
    parse_sql(src, column_indices, cudf_types, counter, host_cols, null_masks);
    counter++;
  }

  sqlReturn = SQLFreeHandle(SQL_HANDLE_STMT, sqlHStmt);
  if (sqlReturn == SQL_ERROR) {
    throw std::runtime_error("SnowFlake: parsing loop");
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
  return ParseCudfType<std::int8_t>(src, col, row, v);
}
std::uint8_t snowflake_parser::parse_cudf_int16(void *,
                                                std::size_t,
                                                std::size_t,
                                                std::vector<std::int16_t> *) {
  return ParseCudfType<std::int16_t>(src, col, row, v);
}
std::uint8_t snowflake_parser::parse_cudf_int32(void *,
                                                std::size_t,
                                                std::size_t,
                                                std::vector<std::int32_t> *) {
  return ParseCudfType<std::int32_t>(src, col, row, v);
}
std::uint8_t snowflake_parser::parse_cudf_int64(void * src,
                                                std::size_t col,
                                                std::size_t row,
                                                std::vector<std::int64_t> * v) {
  return ParseCudfType<std::int64_t>(src, col, row, v);
}

std::uint8_t snowflake_parser::parse_cudf_uint8(void *,
                                                std::size_t,
                                                std::size_t,
                                                std::vector<std::uint8_t> *) {
  throw std::runtime_error("Unsupported type uint8");
}
std::uint8_t snowflake_parser::parse_cudf_uint16(void *,
                                                 std::size_t,
                                                 std::size_t,
                                                 std::vector<std::uint16_t> *) {
  throw std::runtime_error("Unsupported type uint16");
}
std::uint8_t snowflake_parser::parse_cudf_uint32(void *,
                                                 std::size_t,
                                                 std::size_t,
                                                 std::vector<std::uint32_t> *) {
  throw std::runtime_error("Unsupported type uint3");
}
std::uint8_t snowflake_parser::parse_cudf_uint64(void *,
                                                 std::size_t,
                                                 std::size_t,
                                                 std::vector<std::uint64_t> *) {
  throw std::runtime_error("Unsupported type uint64");
}
std::uint8_t snowflake_parser::parse_cudf_float32(void *,
                                                  std::size_t,
                                                  std::size_t,
                                                  std::vector<float> *) {
  throw std::runtime_error("Unsupported type float32");
}
std::uint8_t snowflake_parser::parse_cudf_float64(void *,
                                                  std::size_t,
                                                  std::size_t,
                                                  std::vector<double> *) {
  throw std::runtime_error("Unsupported type float64");
}
std::uint8_t snowflake_parser::parse_cudf_bool8(void *,
                                                std::size_t,
                                                std::size_t,
                                                std::vector<std::int8_t> *) {
  throw std::runtime_error("Unsupported type bool8");
}
std::uint8_t snowflake_parser::parse_cudf_timestamp_days(void *,
                                                         std::size_t,
                                                         std::size_t,
                                                         cudf_string_col *) {
  throw std::runtime_error("Unsupported type timestamp days");
}
std::uint8_t snowflake_parser::parse_cudf_timestamp_seconds(void *,
                                                            std::size_t,
                                                            std::size_t,
                                                            cudf_string_col *) {
  throw std::runtime_error("Unsupported type timestamp seconds");
}
std::uint8_t
snowflake_parser::parse_cudf_timestamp_milliseconds(void *,
                                                    std::size_t,
                                                    std::size_t,
                                                    cudf_string_col *) {
  throw std::runtime_error("Unsupported type timestamp milliseconds");
}
std::uint8_t
snowflake_parser::parse_cudf_timestamp_microseconds(void *,
                                                    std::size_t,
                                                    std::size_t,
                                                    cudf_string_col *) {
  throw std::runtime_error("Unsupported type timestamp microseconds");
}
std::uint8_t
snowflake_parser::parse_cudf_timestamp_nanoseconds(void *,
                                                   std::size_t,
                                                   std::size_t,
                                                   cudf_string_col *) {
  throw std::runtime_error("Unsupported type timestamp nanoseconds");
}
std::uint8_t snowflake_parser::parse_cudf_string(void *,
                                                 std::size_t,
                                                 std::size_t,
                                                 cudf_string_col *) {
  throw std::runtime_error("Unsupported type string");
}


}  // namespace io
}  // namespace ral
