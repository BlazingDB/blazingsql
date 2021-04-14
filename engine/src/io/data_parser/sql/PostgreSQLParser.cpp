/*
 * Copyright 2021 BlazingDB, Inc.
 *     Copyright 2021 Cristhian Alberto Gonzales Castillo
 * <cristhian@voltrondata.com>
 */

#include <array>
#include <iomanip>
#include <sstream>

#include <cudf/io/types.hpp>
#include <cudf/utilities/bit.hpp>
#include <libpq-fe.h>
#include <netinet/in.h>

#include "PostgreSQLParser.h"
#include "sqlcommon.h"

// TODO(cristhian): To optimize read about ECPG postgresql

namespace ral {
namespace io {

static const std::array<const char *, 6> postgresql_string_type_hints = {
  "character", "character varying", "bytea", "text", "anyarray", "name"};

static inline bool postgresql_is_cudf_string(const std::string & hint) {
  const auto * result = std::find_if(std::cbegin(postgresql_string_type_hints),
    std::cend(postgresql_string_type_hints),
    [&hint](
      const char * c_hint) { return std::strcmp(c_hint, hint.c_str()) == 0; });
  return result != std::cend(postgresql_string_type_hints);
}

static inline std::string date_to_string(std::int32_t jd) {
  // For date conversion see
  // https://doxygen.postgresql.org/backend_2utils_2adt_2datetime_8c.html#a889d375aaf2a25be071d818565142b9e
  std::uint32_t julian, quad, extra;
  std::int32_t y;
  julian = static_cast<std::uint32_t>(jd);
  julian += 32044;
  quad = julian / 146097;
  extra = (julian - quad * 146097) * 4 + 3;
  julian += 60 + quad * 3 + extra / 146097;
  quad = julian / 1461;
  julian -= quad * 1461;
  y = julian * 4 / 1461;
  julian = ((y != 0) ? (julian + 305) % 365 : (julian + 306) % 366) + 123;
  y += quad * 4;
  std::int32_t year = y - 4800;
  quad = julian * 2141 / 65536;
  std::int32_t day = julian - 7834 * quad / 256;
  std::int32_t month = (quad + 10) % 12 + 1;
  std::stringstream oss;
  oss << year << '-' << std::setfill('0') << std::setw(2) << month << '-'
      << std::setw(2) << day;
  return oss.str();
}

static inline cudf::type_id parse_postgresql_column_type(
  const std::string & columnTypeName) {
  if (postgresql_is_cudf_string(columnTypeName)) {
    return cudf::type_id::STRING;
  }
  if (columnTypeName == "smallint") { return cudf::type_id::INT16; }
  if (columnTypeName == "integer") { return cudf::type_id::INT32; }
  if (columnTypeName == "bigint") { return cudf::type_id::INT64; }
  if (columnTypeName == "decimal") { return cudf::type_id::DECIMAL64; }
  if (columnTypeName == "numeric") { return cudf::type_id::DECIMAL64; }
  if (columnTypeName == "real") { return cudf::type_id::FLOAT32; }
  if (columnTypeName == "double precision") { return cudf::type_id::FLOAT64; }
  if (columnTypeName == "smallserial") { return cudf::type_id::INT16; }
  if (columnTypeName == "serial") { return cudf::type_id::INT32; }
  if (columnTypeName == "bigserial") { return cudf::type_id::INT64; }
  if (columnTypeName == "boolean") { return cudf::type_id::BOOL8; }
  if (columnTypeName == "date") { return cudf::type_id::TIMESTAMP_DAYS; }
  if (columnTypeName == "money") { return cudf::type_id::UINT64; }
  if (columnTypeName == "timestamp without time zone") {
    return cudf::type_id::TIMESTAMP_MICROSECONDS;
  }
  if (columnTypeName == "timestamp with time zone") {
    return cudf::type_id::TIMESTAMP_MICROSECONDS;
  }
  if (columnTypeName == "time without time zone") {
    return cudf::type_id::DURATION_MICROSECONDS;
  }
  if (columnTypeName == "time with time zone") {
    return cudf::type_id::DURATION_MICROSECONDS;
  }
  if (columnTypeName == "interval") {
    return cudf::type_id::DURATION_MICROSECONDS;
  }
  if (columnTypeName == "inet") { return cudf::type_id::UINT64; }
  if (columnTypeName == "USER-DEFINED") { return cudf::type_id::STRUCT; }
  if (columnTypeName == "ARRAY") { return cudf::type_id::LIST; }
  throw std::runtime_error("PostgreSQL type hint not found: " + columnTypeName);
}

postgresql_parser::postgresql_parser()
    : abstractsql_parser{DataType::POSTGRESQL} {}

postgresql_parser::~postgresql_parser() = default;

void postgresql_parser::read_sql_loop(void * src,
  const std::vector<cudf::type_id> & cudf_types,
  const std::vector<int> & column_indices,
  std::vector<void *> & host_cols,
  std::vector<std::vector<cudf::bitmask_type>> & null_masks) {
  PGresult * result = static_cast<PGresult *>(src);
  const int ntuples = PQntuples(result);
  for (int rowCounter = 0; rowCounter < ntuples; rowCounter++) {
    parse_sql(
      src, column_indices, cudf_types, rowCounter, host_cols, null_masks);
  }
}

cudf::type_id postgresql_parser::get_cudf_type_id(
  const std::string & sql_column_type) {
  return parse_postgresql_column_type(sql_column_type);
}

std::uint8_t postgresql_parser::parse_cudf_int8(
  void * src, std::size_t col, std::size_t row, std::vector<std::int8_t> * v) {
  PGresult * pgResult = static_cast<PGresult *>(src);
  if (PQgetisnull(pgResult, row, col)) { return 0; }
  const char * result = PQgetvalue(pgResult, row, col);
  const std::int8_t value = *reinterpret_cast<const std::int8_t *>(result);
  v->at(row) = value;
  return 1;
}

std::uint8_t postgresql_parser::parse_cudf_int16(
  void * src, std::size_t col, std::size_t row, std::vector<std::int16_t> * v) {
  PGresult * pgResult = static_cast<PGresult *>(src);
  if (PQgetisnull(pgResult, row, col)) { return 0; }
  const char * result = PQgetvalue(pgResult, row, col);
  const std::int16_t value =
    ntohs(*reinterpret_cast<const std::int16_t *>(result));
  v->at(row) = value;
  return 1;
}

std::uint8_t postgresql_parser::parse_cudf_int32(
  void * src, std::size_t col, std::size_t row, std::vector<std::int32_t> * v) {
  PGresult * pgResult = static_cast<PGresult *>(src);
  if (PQgetisnull(pgResult, row, col)) { return 0; }
  const char * result = PQgetvalue(pgResult, row, col);
  const std::int32_t value =
    ntohl(*reinterpret_cast<const std::int32_t *>(result));
  v->at(row) = value;
  return 1;
}

std::uint8_t postgresql_parser::parse_cudf_int64(
  void * src, std::size_t col, std::size_t row, std::vector<std::int64_t> * v) {
  PGresult * pgResult = static_cast<PGresult *>(src);
  if (PQgetisnull(pgResult, row, col)) { return 0; }
  const char * result = PQgetvalue(pgResult, row, col);
  const std::int64_t value =
    __builtin_bswap64(*reinterpret_cast<const std::int64_t *>(result));
  v->at(row) = value;
  return 1;
}

std::uint8_t postgresql_parser::parse_cudf_uint8(
  void * src, std::size_t col, std::size_t row, std::vector<std::uint8_t> * v) {
  PGresult * pgResult = static_cast<PGresult *>(src);
  if (PQgetisnull(pgResult, row, col)) { return 0; }
  const char * result = PQgetvalue(pgResult, row, col);
  const std::uint8_t value = *reinterpret_cast<const std::uint8_t *>(result);
  v->at(row) = value;
  return 1;
}

std::uint8_t postgresql_parser::parse_cudf_uint16(void * src,
  std::size_t col,
  std::size_t row,
  std::vector<std::uint16_t> * v) {
  PGresult * pgResult = static_cast<PGresult *>(src);
  if (PQgetisnull(pgResult, row, col)) { return 0; }
  const char * result = PQgetvalue(pgResult, row, col);
  const std::uint16_t value =
    ntohs(*reinterpret_cast<const std::uint16_t *>(result));
  v->at(row) = value;
  return 1;
}

std::uint8_t postgresql_parser::parse_cudf_uint32(void * src,
  std::size_t col,
  std::size_t row,
  std::vector<std::uint32_t> * v) {
  PGresult * pgResult = static_cast<PGresult *>(src);
  if (PQgetisnull(pgResult, row, col)) { return 0; }
  const char * result = PQgetvalue(pgResult, row, col);
  const std::uint32_t value =
    ntohl(*reinterpret_cast<const std::uint32_t *>(result));
  v->at(row) = value;
  return 1;
}

std::uint8_t postgresql_parser::parse_cudf_uint64(void * src,
  std::size_t col,
  std::size_t row,
  std::vector<std::uint64_t> * v) {
  PGresult * pgResult = static_cast<PGresult *>(src);
  if (PQgetisnull(pgResult, row, col)) { return 0; }
  const char * result = PQgetvalue(pgResult, row, col);
  const std::uint64_t value =
    __builtin_bswap64(*reinterpret_cast<const std::int64_t *>(result));
  v->at(row) = value;
  return 1;
}

std::uint8_t postgresql_parser::parse_cudf_float32(
  void * src, std::size_t col, std::size_t row, std::vector<float> * v) {
  PGresult * pgResult = static_cast<PGresult *>(src);
  if (PQgetisnull(pgResult, row, col)) { return 0; }
  const char * result = PQgetvalue(pgResult, row, col);
  const std::int32_t casted =
    ntohl(*reinterpret_cast<const std::int32_t *>(result));
  const float value = *reinterpret_cast<const float *>(&casted);
  v->at(row) = value;
  return 1;
}

std::uint8_t postgresql_parser::parse_cudf_float64(
  void * src, std::size_t col, std::size_t row, std::vector<double> * v) {
  PGresult * pgResult = static_cast<PGresult *>(src);
  if (PQgetisnull(pgResult, row, col)) { return 0; }
  const char * result = PQgetvalue(pgResult, row, col);
  const std::int64_t casted =
    __builtin_bswap64(*reinterpret_cast<const std::int64_t *>(result));
  const double value = *reinterpret_cast<const double *>(&casted);
  v->at(row) = value;
  return 1;
}

std::uint8_t postgresql_parser::parse_cudf_bool8(
  void * src, std::size_t col, std::size_t row, std::vector<std::int8_t> * v) {
  return parse_cudf_int8(src, col, row, v);
}

std::uint8_t postgresql_parser::parse_cudf_timestamp_days(
  void * src, std::size_t col, std::size_t row, cudf_string_col * v) {
  return parse_cudf_string(src, col, row, v);
}

std::uint8_t postgresql_parser::parse_cudf_timestamp_seconds(
  void * src, std::size_t col, std::size_t row, cudf_string_col * v) {
  return parse_cudf_string(src, col, row, v);
}

std::uint8_t postgresql_parser::parse_cudf_timestamp_milliseconds(
  void * src, std::size_t col, std::size_t row, cudf_string_col * v) {
  return parse_cudf_string(src, col, row, v);
}

std::uint8_t postgresql_parser::parse_cudf_timestamp_microseconds(
  void * src, std::size_t col, std::size_t row, cudf_string_col * v) {
  return parse_cudf_string(src, col, row, v);
}

std::uint8_t postgresql_parser::parse_cudf_timestamp_nanoseconds(
  void * src, std::size_t col, std::size_t row, cudf_string_col * v) {
  return parse_cudf_string(src, col, row, v);
}

std::uint8_t postgresql_parser::parse_cudf_string(
  void * src, std::size_t col, std::size_t row, cudf_string_col * v) {
  PGresult * pgResult = static_cast<PGresult *>(src);

  Oid oid = PQftype(pgResult, col);
  if (oid == InvalidOid) { throw std::runtime_error("Bad postgresql type"); }

  if (PQgetisnull(pgResult, row, col)) {
    v->offsets.push_back(v->offsets.back());
    return 0;
  }
  const char * result = PQgetvalue(pgResult, row, col);
  std::string data;

  // TODO(cristhian): convert oid to data type using postgresql pgtype table
  switch (oid) {
  case 1043: {  // text
    data = result;
    break;
  }
  case 1082: {  // date
    const std::uint32_t POSTGRESQL_EPOCH_DATE = 2451545;
    const std::int32_t value =
      ntohl(*reinterpret_cast<const std::int32_t *>(result));
    data = date_to_string(POSTGRESQL_EPOCH_DATE + value);
    break;
  }
  default: throw std::runtime_error("Unsupported oid parse string");
  }

  v->chars.insert(v->chars.end(), data.cbegin(), data.cend());
  v->offsets.push_back(v->offsets.back() + data.length());
  return 1;
}


}  // namespace io
}  // namespace ral
