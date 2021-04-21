/*
 * Copyright 2021 Percy Camilo Triveño Aucahuasi <percy.camilo.ta@gmail.com>
 * Copyright 2021 Cristhian Alberto Gonzales Castillo
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

static inline void date_to_ymd(std::int32_t jd,
  std::int32_t & year,
  std::int32_t & month,
  std::int32_t & day) {
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
  year = y - 4800;
  quad = julian * 2141 / 65536;
  day = julian - 7834 * quad / 256;
  month = (quad + 10) % 12 + 1;
}

static inline void time_to_hms(std::int64_t jd,
  std::int32_t & hour,
  std::int32_t & min,
  std::int32_t & sec,
  std::int32_t & msec) {
  std::int64_t time;
  const std::int64_t USECS_PER_HOUR = 3600000000;
  const std::int64_t USECS_PER_MINUTE = 60000000;
  const std::int64_t USECS_PER_SEC = 1000000;
  time = jd;
  hour = time / USECS_PER_HOUR;
  time -= (hour) *USECS_PER_HOUR;
  min = time / USECS_PER_MINUTE;
  time -= (min) *USECS_PER_MINUTE;
  sec = time / USECS_PER_SEC;
  msec = time - (sec * USECS_PER_SEC);
}

static inline int timestamp_to_tm(
  std::int64_t dt, struct tm & tm, std::int32_t & msec) {
  std::int64_t dDate, POSTGRESQL_EPOCH_DATE = 2451545;
  std::int64_t time;
  std::uint64_t USECS_PER_DAY = 86400000000;
  time = dt;

  dDate = time / USECS_PER_DAY;
  if (dDate != 0) { time -= dDate * USECS_PER_DAY; }

  if (time < 0) {
    time += USECS_PER_DAY;
    dDate -= 1;
  }
  dDate += POSTGRESQL_EPOCH_DATE;

  date_to_ymd(
    static_cast<std::int32_t>(dDate), tm.tm_year, tm.tm_mon, tm.tm_mday);
  time_to_hms(time, tm.tm_hour, tm.tm_min, tm.tm_sec, msec);
}

static inline std::string date_to_string(std::int32_t jd) {
  // For date conversion see
  // https://doxygen.postgresql.org/backend_2utils_2adt_2datetime_8c.html#a889d375aaf2a25be071d818565142b9e
  const std::int32_t POSTGRESQL_EPOCH_DATE = 2451545;
  std::int32_t year, month, day;
  date_to_ymd(POSTGRESQL_EPOCH_DATE + jd, year, month, day);
  std::stringstream oss;
  oss << year << '-' << std::setfill('0') << std::setw(2) << month << '-'
      << std::setw(2) << day;
  return oss.str();
}

static inline std::string timestamp_to_string(std::int64_t tstamp) {
  // For timestamp conversion see
  // https://doxygen.postgresql.org/backend_2utils_2adt_2timestamp_8c.html#a933dc09a38ddcf144a48b2aaf5790893
  struct tm tm;
  std::int32_t msec;
  timestamp_to_tm(tstamp, tm, msec);
  std::stringstream oss;
  oss << tm.tm_year << '-' << std::setfill('0') << std::setw(2) << tm.tm_mon
      << '-' << std::setw(2) << tm.tm_mday << ' ' << std::setw(2) << tm.tm_hour
      << ':' << std::setw(2) << tm.tm_min << ':' << std::setw(2) << tm.tm_sec;
  if (msec != 0) { oss << '.' << std::setw(6) << msec; }
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
    return cudf::type_id::TIMESTAMP_MILLISECONDS;
  }
  if (columnTypeName == "timestamp with time zone") {
    return cudf::type_id::TIMESTAMP_MILLISECONDS;
  }
  if (columnTypeName == "time without time zone") {
    return cudf::type_id::DURATION_MILLISECONDS;
  }
  if (columnTypeName == "time with time zone") {
    return cudf::type_id::DURATION_MILLISECONDS;
  }
  if (columnTypeName == "interval") {
    return cudf::type_id::DURATION_MILLISECONDS;
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
  // https://www.postgresql.org/docs/13/catalog-pg-type.html
  switch (oid) {
  case 1082: {  // date
    const std::int32_t value =
      ntohl(*reinterpret_cast<const std::int32_t *>(result));
    data = date_to_string(value);
    break;
  }
  case 1114: {  // timestamp
    const std::int64_t value =
      __builtin_bswap64(*reinterpret_cast<const std::int64_t *>(result));
    data = timestamp_to_string(value);
    break;
  }
  default: data = result;
  }

  v->chars.insert(v->chars.end(), data.cbegin(), data.cend());
  v->offsets.push_back(v->offsets.back() + data.length());
  return 1;
}


}  // namespace io
}  // namespace ral
