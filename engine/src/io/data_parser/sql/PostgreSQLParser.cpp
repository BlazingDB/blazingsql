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

static const std::array<const char *, 6> postgresql_string_type_hints =
    {"character", "character varying", "bytea", "text", "anyarray", "name"};

static inline bool postgresql_is_cudf_string(const std::string & hint) {
  const auto * result = std::find_if(
      std::cbegin(postgresql_string_type_hints),
      std::cend(postgresql_string_type_hints),
      [&hint](const char * c_hint) { return !hint.rfind(c_hint, 0); });
  return result != std::cend(postgresql_string_type_hints);
}

static inline cudf::type_id
parse_postgresql_column_type(const std::string & columnTypeName) {
  if (postgresql_is_cudf_string(columnTypeName)) {
    return cudf::type_id::STRING;
  }
  if (!columnTypeName.rfind("smallint", 0)) { return cudf::type_id::INT16; }
  if (!columnTypeName.rfind("integer", 0)) { return cudf::type_id::INT32; }
  if (!columnTypeName.rfind("bigint", 0)) { return cudf::type_id::INT64; }
  if (!columnTypeName.rfind("decimal", 0)) { return cudf::type_id::FLOAT64; }
  if (!columnTypeName.rfind("numeric", 0)) { return cudf::type_id::FLOAT64; }
  if (!columnTypeName.rfind("real", 0)) { return cudf::type_id::FLOAT32; }
  if (!columnTypeName.rfind("double precision", 0)) {
    return cudf::type_id::FLOAT64;
  }
  if (!columnTypeName.rfind("smallserial", 0)) { return cudf::type_id::INT16; }
  if (!columnTypeName.rfind("serial", 0)) { return cudf::type_id::INT32; }
  if (!columnTypeName.rfind("bigserial", 0)) { return cudf::type_id::INT64; }
  if (!columnTypeName.rfind("boolean", 0)) { return cudf::type_id::BOOL8; }
  if (!columnTypeName.rfind("date", 0)) {
    return cudf::type_id::TIMESTAMP_MILLISECONDS;
  }
  if (!columnTypeName.rfind("money", 0)) { return cudf::type_id::UINT64; }
  if (!columnTypeName.rfind("timestamp without time zone", 0)) {
    return cudf::type_id::TIMESTAMP_MILLISECONDS;
  }
  if (!columnTypeName.rfind("timestamp with time zone", 0)) {
    return cudf::type_id::TIMESTAMP_MILLISECONDS;
  }
  if (!columnTypeName.rfind("time without time zone", 0)) {
    return cudf::type_id::DURATION_MILLISECONDS;
  }
  if (!columnTypeName.rfind("time with time zone", 0)) {
    return cudf::type_id::DURATION_MILLISECONDS;
  }
  if (!columnTypeName.rfind("interval", 0)) {
    return cudf::type_id::DURATION_MILLISECONDS;
  }
  if (!columnTypeName.rfind("inet", 0)) { return cudf::type_id::UINT64; }
  if (!columnTypeName.rfind("USER-DEFINED", 0)) {
    return cudf::type_id::STRUCT;
  }
  if (!columnTypeName.rfind("ARRAY", 0)) { return cudf::type_id::LIST; }
  throw std::runtime_error("PostgreSQL type hint not found: " + columnTypeName);
}

postgresql_parser::postgresql_parser()
    : abstractsql_parser{DataType::POSTGRESQL} {}

postgresql_parser::~postgresql_parser() = default;

void postgresql_parser::read_sql_loop(
    void * src,
    const std::vector<cudf::type_id> & cudf_types,
    const std::vector<int> & column_indices,
    std::vector<void *> & host_cols,
    std::vector<std::vector<cudf::bitmask_type>> & null_masks) {
  PGresult * result = static_cast<PGresult *>(src);
  const int ntuples = PQntuples(result);
  for (int rowCounter = 0; rowCounter < ntuples; rowCounter++) {
    parse_sql(src,
              column_indices,
              cudf_types,
              rowCounter,
              host_cols,
              null_masks);
  }
}

cudf::type_id
postgresql_parser::get_cudf_type_id(const std::string & sql_column_type) {
  return parse_postgresql_column_type(sql_column_type);
}

std::uint8_t postgresql_parser::parse_cudf_int8(void * src,
                                                std::size_t col,
                                                std::size_t row,
                                                std::vector<std::int8_t> * v) {
  PGresult * pgResult = static_cast<PGresult *>(src);
  if (PQgetisnull(pgResult, row, col)) { return 0; }
  const char * result = PQgetvalue(pgResult, row, col);
  char * end;
  const std::int8_t value =
      static_cast<std::int8_t>(std::strtol(result, &end, 10));
  v->at(row) = value;
  return 1;
}

std::uint8_t
postgresql_parser::parse_cudf_int16(void * src,
                                    std::size_t col,
                                    std::size_t row,
                                    std::vector<std::int16_t> * v) {
  PGresult * pgResult = static_cast<PGresult *>(src);
  if (PQgetisnull(pgResult, row, col)) { return 0; }
  const char * result = PQgetvalue(pgResult, row, col);
  char * end;
  const std::int16_t value =
      static_cast<std::int16_t>(std::strtol(result, &end, 10));
  v->at(row) = value;
  return 1;
}

std::uint8_t
postgresql_parser::parse_cudf_int32(void * src,
                                    std::size_t col,
                                    std::size_t row,
                                    std::vector<std::int32_t> * v) {
  PGresult * pgResult = static_cast<PGresult *>(src);
  if (PQgetisnull(pgResult, row, col)) { return 0; }
  const char * result = PQgetvalue(pgResult, row, col);
  char * end;
  const std::int32_t value =
      static_cast<std::int32_t>(std::strtol(result, &end, 10));
  v->at(row) = value;
  return 1;
}

std::uint8_t
postgresql_parser::parse_cudf_int64(void * src,
                                    std::size_t col,
                                    std::size_t row,
                                    std::vector<std::int64_t> * v) {
  PGresult * pgResult = static_cast<PGresult *>(src);
  if (PQgetisnull(pgResult, row, col)) { return 0; }
  const char * result = PQgetvalue(pgResult, row, col);
  char * end;
  const std::int64_t value =
      static_cast<std::int64_t>(std::strtoll(result, &end, 10));
  v->at(row) = value;
  return 1;
}

std::uint8_t
postgresql_parser::parse_cudf_uint8(void * src,
                                    std::size_t col,
                                    std::size_t row,
                                    std::vector<std::uint8_t> * v) {
  PGresult * pgResult = static_cast<PGresult *>(src);
  if (PQgetisnull(pgResult, row, col)) { return 0; }
  const char * result = PQgetvalue(pgResult, row, col);
  char * end;
  const std::uint8_t value =
      static_cast<std::uint8_t>(std::strtoul(result, &end, 10));
  v->at(row) = value;
  return 1;
}

std::uint8_t
postgresql_parser::parse_cudf_uint16(void * src,
                                     std::size_t col,
                                     std::size_t row,
                                     std::vector<std::uint16_t> * v) {
  PGresult * pgResult = static_cast<PGresult *>(src);
  if (PQgetisnull(pgResult, row, col)) { return 0; }
  const char * result = PQgetvalue(pgResult, row, col);
  char * end;
  const std::uint16_t value =
      static_cast<std::uint16_t>(std::strtol(result, &end, 10));
  v->at(row) = value;
  return 1;
}

std::uint8_t
postgresql_parser::parse_cudf_uint32(void * src,
                                     std::size_t col,
                                     std::size_t row,
                                     std::vector<std::uint32_t> * v) {
  PGresult * pgResult = static_cast<PGresult *>(src);
  if (PQgetisnull(pgResult, row, col)) { return 0; }
  const char * result = PQgetvalue(pgResult, row, col);
  char * end;
  const std::uint32_t value =
      static_cast<std::uint32_t>(std::strtoul(result, &end, 10));
  v->at(row) = value;
  return 1;
}

std::uint8_t
postgresql_parser::parse_cudf_uint64(void * src,
                                     std::size_t col,
                                     std::size_t row,
                                     std::vector<std::uint64_t> * v) {
  PGresult * pgResult = static_cast<PGresult *>(src);
  if (PQgetisnull(pgResult, row, col)) { return 0; }
  const char * result = PQgetvalue(pgResult, row, col);
  char * end;
  const std::uint64_t value =
      static_cast<std::uint64_t>(std::strtoull(result, &end, 10));
  v->at(row) = value;
  return 1;
}

std::uint8_t postgresql_parser::parse_cudf_float32(void * src,
                                                   std::size_t col,
                                                   std::size_t row,
                                                   std::vector<float> * v) {
  PGresult * pgResult = static_cast<PGresult *>(src);
  if (PQgetisnull(pgResult, row, col)) { return 0; }
  const char * result = PQgetvalue(pgResult, row, col);
  char * end;
  const float value = static_cast<float>(std::strtof(result, &end));
  v->at(row) = value;
  return 1;
}

std::uint8_t postgresql_parser::parse_cudf_float64(void * src,
                                                   std::size_t col,
                                                   std::size_t row,
                                                   std::vector<double> * v) {
  PGresult * pgResult = static_cast<PGresult *>(src);
  if (PQgetisnull(pgResult, row, col)) { return 0; }
  const char * result = PQgetvalue(pgResult, row, col);
  char * end;
  const double value = static_cast<double>(std::strtod(result, &end));
  v->at(row) = value;
  return 1;
}

std::uint8_t postgresql_parser::parse_cudf_bool8(void * src,
                                                 std::size_t col,
                                                 std::size_t row,
                                                 std::vector<std::int8_t> * v) {
  PGresult * pgResult = static_cast<PGresult *>(src);
  if (PQgetisnull(pgResult, row, col)) { return 0; }
  const std::string result = std::string{PQgetvalue(pgResult, row, col)};
  std::int8_t value;
  if (result == "t") {
    value = 1;
  } else {
    if (result == "f") {
      value = 0;
    } else {
      return 0;
    }
  }
  v->at(row) = value;
  return 1;
}

std::uint8_t postgresql_parser::parse_cudf_timestamp_days(void * src,
                                                          std::size_t col,
                                                          std::size_t row,
                                                          cudf_string_col * v) {
  return parse_cudf_string(src, col, row, v);
}

std::uint8_t
postgresql_parser::parse_cudf_timestamp_seconds(void * src,
                                                std::size_t col,
                                                std::size_t row,
                                                cudf_string_col * v) {
  return parse_cudf_string(src, col, row, v);
}

std::uint8_t
postgresql_parser::parse_cudf_timestamp_milliseconds(void * src,
                                                     std::size_t col,
                                                     std::size_t row,
                                                     cudf_string_col * v) {
  return parse_cudf_string(src, col, row, v);
}

std::uint8_t
postgresql_parser::parse_cudf_timestamp_microseconds(void * src,
                                                     std::size_t col,
                                                     std::size_t row,
                                                     cudf_string_col * v) {
  return parse_cudf_string(src, col, row, v);
}

std::uint8_t
postgresql_parser::parse_cudf_timestamp_nanoseconds(void * src,
                                                    std::size_t col,
                                                    std::size_t row,
                                                    cudf_string_col * v) {
  return parse_cudf_string(src, col, row, v);
}

std::uint8_t postgresql_parser::parse_cudf_string(void * src,
                                                  std::size_t col,
                                                  std::size_t row,
                                                  cudf_string_col * v) {
  PGresult * pgResult = static_cast<PGresult *>(src);

  if (PQgetisnull(pgResult, row, col)) {
    v->offsets.push_back(v->offsets.back());
    return 0;
  }
  const char * result = PQgetvalue(pgResult, row, col);
  std::string data = result;

  // trim spaces because postgresql store chars with padding.
  Oid oid = PQftype(pgResult, col);
  if (oid == InvalidOid) { throw std::runtime_error("Bad postgresql type"); }
  if (oid == static_cast<Oid>(1042)) {
    data.erase(std::find_if(data.rbegin(),
                            data.rend(),
                            [](unsigned char c) { return !std::isspace(c); })
                   .base(),
               data.end());
  }

  v->chars.insert(v->chars.end(), data.cbegin(), data.cend());
  v->offsets.push_back(v->offsets.back() + data.length());
  return 1;
}


}  // namespace io
}  // namespace ral
