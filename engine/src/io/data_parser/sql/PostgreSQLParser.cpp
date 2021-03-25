/*
 * Copyright 2021 BlazingDB, Inc.
 *     Copyright 2021 Cristhian Alberto Gonzales Castillo
 * <cristhian@blazingdb.com>
 */

#include <array>

#include "PostgreSQLParser.h"

namespace ral {
namespace io {

static const std::array<const char *, 8> postgresql_string_type_hints = {
    "character",
    "character varying",
    "money",
    "bytea",
    "text",
    "anyarray",
    "numeric",
    "name"};

static inline bool postgresql_is_cudf_string(const std::string &hint) {
  const auto *result =
      std::find_if(std::cbegin(postgresql_string_type_hints),
                   std::cend(postgresql_string_type_hints),
                   [&hint](const char *c_hint) {
                     return std::strcmp(c_hint, hint.c_str()) == 0;
                   });
  return result != std::cend(postgresql_string_type_hints);
}

static inline cudf::type_id
MapPostgreSQLTypeName(const std::string &columnTypeName) {
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
  throw std::runtime_error("PostgreSQL type hint not found: " + columnTypeName);
}

postgresql_parser::postgresql_parser() = default;

postgresql_parser::~postgresql_parser() = default;

std::unique_ptr<frame::BlazingTable>
postgresql_parser::parse_batch(data_handle handle,
                               const Schema &schema,
                               std::vector<int> column_indices,
                               std::vector<cudf::size_type> row_groups) {
  // Here I need the result set from postgresl
  return nullptr;
}

void postgresql_parser::parse_schema(data_handle handle, Schema &schema) {
  const bool is_in_file = true;
  std::size_t file_index = 0;
  const std::size_t columnsLength = handle.sql_handle.column_names.size();
  for (std::size_t i = 0; i < columnsLength; i++) {
    const std::string &column_type = handle.sql_handle.column_types.at(i);
    cudf::type_id type = MapPostgreSQLTypeName(column_type);
    const std::string &name = handle.sql_handle.column_names.at(i);
    schema.add_column(name, type, file_index++, is_in_file);
  }
}

std::unique_ptr<frame::BlazingTable>
postgresql_parser::get_metadata(std::vector<data_handle> handles, int offset) {
  return nullptr;
}

}  // namespace io
}  // namespace ral
