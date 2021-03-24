/*
 * Copyright 2021 BlazingDB, Inc.
 *     Copyright 2021 Cristhian Alberto Gonzales Castillo
 * <cristhian@blazingdb.com>
 */

#include "PostgreSQLParser.h"

namespace ral {
namespace io {

namespace {

cudf::type_id MapPostgreSQLType(const std::string &columnType) {
  // TODO(cristhian): complete type list
  std::unordered_map<std::string, cudf::type_id> postgreSQL2CudfUmap{
      {"", cudf::type_id::EMPTY},
      {"int4", cudf::type_id::INT64},
      {"text", cudf::type_id::STRING},
  };
  // TODO NOTE: move to switch statement
  try {
    return postgreSQL2CudfUmap.at(columnType);
  } catch (const std::out_of_range &) {
    std::cerr << "Unsupported postgreSQL column type: " << columnType
              << std::endl;
    throw std::runtime_error("Unsupported postgreSQL column type");
  }
}

}  // namespace

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
    cudf::type_id type = MapPostgreSQLType(column_type);
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
