/*
 * Copyright 2021 BlazingDB, Inc.
 *     Copyright 2021 Cristhian Alberto Gonzales Castillo
 * <cristhian@voltrondata.com>
 */

#include <array>

#include <cudf/io/types.hpp>
#include <cudf/utilities/bit.hpp>
#include <libpq-fe.h>
#include <netinet/in.h>

#include "PostgreSQLParser.h"
#include "sqlcommon.h"

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

static inline cudf::io::table_with_metadata read_postgresql(
  const std::shared_ptr<PGresult> & pgResult,
  const std::vector<int> & column_indices,
  const std::vector<cudf::type_id> & cudf_types,
  const std::vector<std::size_t> & column_bytes) {
  const std::size_t resultNfields = PQnfields(pgResult.get());
  if (resultNfields != column_indices.size() ||
      resultNfields != column_bytes.size() ||
      resultNfields != cudf_types.size()) {
    throw std::runtime_error(
      "Not equal columns for indices and bytes in PostgreSQL filter");
  }

  std::vector<void *> host_cols;
  host_cols.reserve(resultNfields);
  const int resultNtuples = PQntuples(pgResult.get());
  const std::size_t bitmask_allocation =
    cudf::bitmask_allocation_size_bytes(resultNtuples);
  const std::size_t num_words = bitmask_allocation / sizeof(cudf::bitmask_type);
  std::vector<std::vector<cudf::bitmask_type>> null_masks(resultNfields);
  std::transform(column_indices.cbegin(),
    column_indices.cend(),
    std::back_inserter(host_cols),
    [&pgResult, &cudf_types, &null_masks, num_words, resultNtuples](
      const int projection_index) {
      null_masks[projection_index].resize(num_words, 0);
      const int fsize = PQfsize(pgResult.get(), projection_index);
      if (fsize < 0) {  // STRING, STRUCT, LIST, and similar cases
        auto * string_col = new cudf_string_col();
        string_col->offsets.reserve(resultNtuples + 1);
        string_col->offsets.push_back(0);
        return static_cast<void *>(string_col);
      }
      // primitives cases
      const cudf::type_id cudf_type_id = cudf_types[projection_index];
      switch (cudf_type_id) {
      case cudf::type_id::INT8: {
        auto * vector = new std::vector<std::int8_t>;
        vector->reserve(resultNtuples);
        return static_cast<void *>(vector);
      }
      case cudf::type_id::INT16: {
        auto * vector = new std::vector<std::int16_t>;
        vector->reserve(resultNtuples);
        return static_cast<void *>(vector);
      }
      case cudf::type_id::INT32: {
        auto * vector = new std::vector<std::int32_t>;
        vector->reserve(resultNtuples);
        return static_cast<void *>(vector);
      }
      case cudf::type_id::INT64: {
        auto * vector = new std::vector<std::int64_t>;
        vector->reserve(resultNtuples);
        return static_cast<void *>(vector);
      }
      case cudf::type_id::UINT8: {
        auto * vector = new std::vector<std::uint8_t>;
        vector->reserve(resultNtuples);
        return static_cast<void *>(vector);
      }
      case cudf::type_id::UINT16: {
        auto * vector = new std::vector<std::uint16_t>;
        vector->reserve(resultNtuples);
        return static_cast<void *>(vector);
      }
      case cudf::type_id::UINT32: {
        auto * vector = new std::vector<std::uint32_t>;
        vector->reserve(resultNtuples);
        return static_cast<void *>(vector);
      }
      case cudf::type_id::UINT64: {
        auto * vector = new std::vector<std::uint64_t>;
        vector->reserve(resultNtuples);
        return static_cast<void *>(vector);
      }
      case cudf::type_id::FLOAT32:
      case cudf::type_id::DECIMAL32: {
        auto * vector = new std::vector<float>;
        vector->reserve(resultNtuples);
        return static_cast<void *>(vector);
      }
      case cudf::type_id::FLOAT64:
      case cudf::type_id::DECIMAL64: {
        auto * vector = new std::vector<double>;
        vector->reserve(resultNtuples);
        return static_cast<void *>(vector);
      }
      case cudf::type_id::BOOL8: {
        auto * vector = new std::vector<std::uint8_t>;
        vector->reserve(resultNtuples);
        return static_cast<void *>(vector);
      }
      case cudf::type_id::STRING: {
        auto * string_col = new cudf_string_col();
        string_col->offsets.reserve(resultNtuples + 1);
        string_col->offsets.push_back(0);
        return static_cast<void *>(string_col);
      }
      default: throw std::runtime_error("Invalid allocation for cudf type id");
      }
    });

  for (int i = 0; i < resultNtuples; i++) {
    for (const std::size_t projection_index : column_indices) {
      cudf::type_id cudf_type_id = cudf_types[projection_index];
      const char * resultValue =
        PQgetvalue(pgResult.get(), i, projection_index);
      const bool isNull =
        static_cast<bool>(PQgetisnull(pgResult.get(), i, projection_index));
      switch (cudf_type_id) {
      case cudf::type_id::INT8: {
        const std::int8_t castedValue =
          *reinterpret_cast<const std::int8_t *>(resultValue);
        std::vector<std::int8_t> & vector =
          *reinterpret_cast<std::vector<std::int8_t> *>(
            host_cols[projection_index]);
        vector.push_back(castedValue);
        break;
      }
      case cudf::type_id::INT16: {
        const std::int16_t castedValue =
          *reinterpret_cast<const std::int16_t *>(resultValue);
        const std::int16_t hostOrderedValue = ntohs(castedValue);
        std::vector<std::int16_t> & vector =
          *reinterpret_cast<std::vector<std::int16_t> *>(
            host_cols[projection_index]);
        vector.push_back(hostOrderedValue);
        break;
      }
      case cudf::type_id::INT32: {
        const std::int32_t castedValue =
          *reinterpret_cast<const std::int32_t *>(resultValue);
        const std::int32_t hostOrderedValue = ntohl(castedValue);
        std::vector<std::int32_t> & vector =
          *reinterpret_cast<std::vector<std::int32_t> *>(
            host_cols[projection_index]);
        vector.push_back(hostOrderedValue);
        break;
      }
      case cudf::type_id::INT64: {
        const std::int64_t castedValue =
          *reinterpret_cast<const std::int64_t *>(resultValue);
        const std::int64_t hostOrderedValue = ntohl(castedValue);
        std::vector<std::int64_t> & vector =
          *reinterpret_cast<std::vector<std::int64_t> *>(
            host_cols[projection_index]);
        vector.push_back(hostOrderedValue);
        break;
      }
      case cudf::type_id::UINT8: {
        const std::int8_t castedValue =
          *reinterpret_cast<const std::int8_t *>(resultValue);
        std::vector<std::uint8_t> & vector =
          *reinterpret_cast<std::vector<std::uint8_t> *>(
            host_cols[projection_index]);
        vector.push_back(castedValue);
        break;
      }
      case cudf::type_id::UINT16: {
        const std::int16_t castedValue =
          *reinterpret_cast<const std::int16_t *>(resultValue);
        const std::int16_t hostOrderedValue = ntohs(castedValue);
        std::vector<std::uint16_t> & vector =
          *reinterpret_cast<std::vector<std::uint16_t> *>(
            host_cols[projection_index]);
        vector.push_back(hostOrderedValue);
        break;
      }
      case cudf::type_id::UINT32: {
        const std::int32_t castedValue =
          *reinterpret_cast<const std::int32_t *>(resultValue);
        const std::int32_t hostOrderedValue = ntohl(castedValue);
        std::vector<std::uint32_t> & vector =
          *reinterpret_cast<std::vector<std::uint32_t> *>(
            host_cols[projection_index]);
        vector.push_back(hostOrderedValue);
        break;
      }
      case cudf::type_id::UINT64: {
        const std::int64_t castedValue =
          *reinterpret_cast<const std::int64_t *>(resultValue);
        const std::int64_t hostOrderedValue = ntohl(castedValue);
        std::vector<std::uint64_t> & vector =
          *reinterpret_cast<std::vector<std::uint64_t> *>(
            host_cols[projection_index]);
        vector.push_back(hostOrderedValue);
        break;
      }
      case cudf::type_id::FLOAT32:
      case cudf::type_id::DECIMAL32: {
        const std::int32_t castedValue =
          *reinterpret_cast<const std::int32_t *>(resultValue);
        const std::int32_t hostOrderedValue = ntohl(castedValue);
        const float floatCastedValue =
          *reinterpret_cast<const float *>(&hostOrderedValue);
        std::vector<float> & vector =
          *reinterpret_cast<std::vector<float> *>(host_cols[projection_index]);
        vector.push_back(floatCastedValue);
        break;
      }
      case cudf::type_id::FLOAT64:
      case cudf::type_id::DECIMAL64: {
        const std::int64_t castedValue =
          *reinterpret_cast<const std::int64_t *>(resultValue);
        const std::int64_t hostOrderedValue = ntohl(castedValue);
        const double doubleCastedValue =
          *reinterpret_cast<const double *>(&hostOrderedValue);
        std::vector<double> & vector =
          *reinterpret_cast<std::vector<double> *>(host_cols[projection_index]);
        vector.push_back(doubleCastedValue);
        break;
      }
      case cudf::type_id::BOOL8: {
        const std::uint8_t castedValue =
          *reinterpret_cast<const std::uint8_t *>(resultValue);
        std::vector<std::uint8_t> & vector =
          *reinterpret_cast<std::vector<std::uint8_t> *>(
            host_cols[projection_index]);
        vector.push_back(castedValue);
        break;
      }
      case cudf::type_id::STRING: {
        cudf_string_col * string_col =
          reinterpret_cast<cudf_string_col *>(host_cols[projection_index]);
        if (isNull) {
          string_col->offsets.push_back(string_col->offsets.back());
        } else {
          std::string data(resultValue);
          string_col->chars.insert(
            string_col->chars.end(), data.cbegin(), data.cend());
          string_col->offsets.push_back(
            string_col->offsets.back() + data.length());
        }
        break;
      }
      default: throw std::runtime_error("Invalid cudf type id");
      }
      if (isNull) {
        cudf::set_bit_unsafe(null_masks[projection_index].data(), i);
      }
    }
  }

  cudf::io::table_with_metadata tableWithMetadata;
  std::vector<std::unique_ptr<cudf::column>> cudf_columns;
  cudf_columns.resize(static_cast<std::size_t>(resultNfields));
  for (const std::size_t projection_index : column_indices) {
    cudf::type_id cudf_type_id = cudf_types[projection_index];
    switch (cudf_type_id) {
    case cudf::type_id::INT8: {
      std::vector<std::int8_t> * vector =
        reinterpret_cast<std::vector<std::int8_t> *>(
          host_cols[projection_index]);
      cudf_columns[projection_index] = build_fixed_width_cudf_col(
        resultNtuples, vector, null_masks[projection_index], cudf_type_id);
      break;
    }
    case cudf::type_id::INT16: {
      std::vector<std::int16_t> * vector =
        reinterpret_cast<std::vector<std::int16_t> *>(
          host_cols[projection_index]);
      cudf_columns[projection_index] = build_fixed_width_cudf_col(
        resultNtuples, vector, null_masks[projection_index], cudf_type_id);
      break;
    }
    case cudf::type_id::INT32: {
      std::vector<std::int32_t> * vector =
        reinterpret_cast<std::vector<std::int32_t> *>(
          host_cols[projection_index]);
      cudf_columns[projection_index] = build_fixed_width_cudf_col(
        resultNtuples, vector, null_masks[projection_index], cudf_type_id);
      break;
    }
    case cudf::type_id::INT64: {
      std::vector<std::int64_t> * vector =
        reinterpret_cast<std::vector<std::int64_t> *>(
          host_cols[projection_index]);
      cudf_columns[projection_index] = build_fixed_width_cudf_col(
        resultNtuples, vector, null_masks[projection_index], cudf_type_id);
      break;
    }
    case cudf::type_id::UINT8: {
      std::vector<std::uint8_t> * vector =
        reinterpret_cast<std::vector<std::uint8_t> *>(
          host_cols[projection_index]);
      cudf_columns[projection_index] = build_fixed_width_cudf_col(
        resultNtuples, vector, null_masks[projection_index], cudf_type_id);
      break;
    }
    case cudf::type_id::UINT16: {
      std::vector<std::uint16_t> * vector =
        reinterpret_cast<std::vector<std::uint16_t> *>(
          host_cols[projection_index]);
      cudf_columns[projection_index] = build_fixed_width_cudf_col(
        resultNtuples, vector, null_masks[projection_index], cudf_type_id);
      break;
    }
    case cudf::type_id::UINT32: {
      std::vector<std::uint32_t> * vector =
        reinterpret_cast<std::vector<std::uint32_t> *>(
          host_cols[projection_index]);
      cudf_columns[projection_index] = build_fixed_width_cudf_col(
        resultNtuples, vector, null_masks[projection_index], cudf_type_id);
      break;
    }
    case cudf::type_id::UINT64: {
      std::vector<std::uint64_t> * vector =
        reinterpret_cast<std::vector<std::uint64_t> *>(
          host_cols[projection_index]);
      cudf_columns[projection_index] = build_fixed_width_cudf_col(
        resultNtuples, vector, null_masks[projection_index], cudf_type_id);
      break;
    }
    case cudf::type_id::FLOAT32:
    case cudf::type_id::DECIMAL32: {
      std::vector<float> * vector =
        reinterpret_cast<std::vector<float> *>(host_cols[projection_index]);
      cudf_columns[projection_index] = build_fixed_width_cudf_col(
        resultNtuples, vector, null_masks[projection_index], cudf_type_id);
      break;
    }
    case cudf::type_id::FLOAT64:
    case cudf::type_id::DECIMAL64: {
      std::vector<double> * vector =
        reinterpret_cast<std::vector<double> *>(host_cols[projection_index]);
      cudf_columns[projection_index] = build_fixed_width_cudf_col(
        resultNtuples, vector, null_masks[projection_index], cudf_type_id);
      break;
    }
    case cudf::type_id::BOOL8: {
      std::vector<std::uint8_t> * vector =
        reinterpret_cast<std::vector<std::uint8_t> *>(
          host_cols[projection_index]);
      cudf_columns[projection_index] = build_fixed_width_cudf_col(
        resultNtuples, vector, null_masks[projection_index], cudf_type_id);
      break;
    }
    case cudf::type_id::STRING: {
      cudf_string_col * string_col =
        reinterpret_cast<cudf_string_col *>(host_cols[projection_index]);
      cudf_columns[projection_index] =
        build_str_cudf_col(string_col, null_masks[projection_index]);
      break;
    }
    default: throw std::runtime_error("Invalid cudf type id");
    }
  }

  tableWithMetadata.tbl =
    std::make_unique<cudf::table>(std::move(cudf_columns));

  for (const std::size_t projection_index : column_indices) {
    cudf::type_id cudf_type_id = cudf_types[projection_index];
    switch (cudf_type_id) {
    case cudf::type_id::INT8: {
      delete reinterpret_cast<std::vector<std::int8_t> *>(
        host_cols[projection_index]);
      break;
    }
    case cudf::type_id::INT16: {
      delete reinterpret_cast<std::vector<std::int16_t> *>(
        host_cols[projection_index]);
      break;
    }
    case cudf::type_id::INT32: {
      delete reinterpret_cast<std::vector<std::int32_t> *>(
        host_cols[projection_index]);
      break;
    }
    case cudf::type_id::INT64: {
      delete reinterpret_cast<std::vector<std::int64_t> *>(
        host_cols[projection_index]);
      break;
    }
    case cudf::type_id::UINT8: {
      delete reinterpret_cast<std::vector<std::uint8_t> *>(
        host_cols[projection_index]);
      break;
    }
    case cudf::type_id::UINT16: {
      delete reinterpret_cast<std::vector<std::uint16_t> *>(
        host_cols[projection_index]);
      break;
    }
    case cudf::type_id::UINT32: {
      delete reinterpret_cast<std::vector<std::uint32_t> *>(
        host_cols[projection_index]);
      break;
    }
    case cudf::type_id::UINT64: {
      delete reinterpret_cast<std::vector<std::uint64_t> *>(
        host_cols[projection_index]);
      break;
    }
    case cudf::type_id::FLOAT32:
    case cudf::type_id::DECIMAL32: {
      delete reinterpret_cast<std::vector<float> *>(
        host_cols[projection_index]);
      break;
    }
    case cudf::type_id::FLOAT64:
    case cudf::type_id::DECIMAL64: {
      delete reinterpret_cast<std::vector<double> *>(
        host_cols[projection_index]);
      break;
    }
    case cudf::type_id::BOOL8: {
      delete reinterpret_cast<std::vector<std::uint8_t> *>(
        host_cols[projection_index]);
      break;
    }
    case cudf::type_id::STRING: {
      delete reinterpret_cast<cudf_string_col *>(host_cols[projection_index]);
      break;
    }
    default: throw std::runtime_error("Invalid cudf type id");
    }
  }
  return tableWithMetadata;
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

std::unique_ptr<frame::BlazingTable> parse_batch(data_handle handle,
  const Schema & schema,
  std::vector<int> column_indices,
  std::vector<cudf::size_type> row_groups) {
  auto pgResult = handle.sql_handle.postgresql_result;
  if (!pgResult) { return schema.makeEmptyBlazingTable(column_indices); }

  if (!column_indices.empty()) {
    std::vector<std::string> columnNames;
    columnNames.reserve(column_indices.size());
    std::transform(column_indices.cbegin(),
      column_indices.cend(),
      std::back_inserter(columnNames),
      std::bind1st(std::mem_fun(&Schema::get_name), &schema));

    auto tableWithMetadata = read_postgresql(pgResult,
      column_indices,
      schema.get_dtypes(),
      handle.sql_handle.column_bytes);
    tableWithMetadata.metadata.column_names = columnNames;

    auto table = std::move(tableWithMetadata.tbl);
    return std::make_unique<frame::BlazingTable>(
      std::move(table), tableWithMetadata.metadata.column_names);
  }

  return nullptr;
}

void parse_schema(data_handle handle, Schema & schema) {
  const bool is_in_file = true;
  const std::size_t columnsLength = handle.sql_handle.column_names.size();
  for (std::size_t i = 0; i < columnsLength; i++) {
    const std::string & column_type = handle.sql_handle.column_types.at(i);
    cudf::type_id type = parse_postgresql_column_type(column_type);
    const std::string & name = handle.sql_handle.column_names.at(i);
    schema.add_column(name, type, i, is_in_file);
  }
}

postgresql_parser::postgresql_parser()
    : abstractsql_parser{DataType::POSTGRESQL} {}

postgresql_parser::~postgresql_parser() = default;

void postgresql_parser::read_sql_loop(void * src,
  const std::vector<cudf::type_id> & cudf_types,
  const std::vector<int> & column_indices,
  std::vector<void *> & host_cols,
  std::vector<std::vector<cudf::bitmask_type>> & null_masks) {}

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
  const std::int64_t value = *reinterpret_cast<const std::int64_t *>(result);
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
  const std::uint64_t value = *reinterpret_cast<const std::uint64_t *>(result);
  v->at(row) = value;
  return 1;
}

std::uint8_t postgresql_parser::parse_cudf_float32(
  void * src, std::size_t col, std::size_t row, std::vector<float> * v) {
  PGresult * pgResult = static_cast<PGresult *>(src);
  if (PQgetisnull(pgResult, row, col)) { return 0; }
  const char * result = PQgetvalue(pgResult, row, col);
  const float value = *reinterpret_cast<const float *>(result);
  v->at(row) = value;
  return 1;
}

std::uint8_t postgresql_parser::parse_cudf_float64(
  void * src, std::size_t col, std::size_t row, std::vector<double> * v) {
  PGresult * pgResult = static_cast<PGresult *>(src);
  if (PQgetisnull(pgResult, row, col)) { return 0; }
  const char * result = PQgetvalue(pgResult, row, col);
  const double value = *reinterpret_cast<const double *>(result);
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
  if (PQgetisnull(pgResult, row, col)) {
    v->offsets.push_back(v->offsets.back());
    return 0;
  } else {
    const char * result = PQgetvalue(pgResult, row, col);
    const std::string data(result);
    v->chars.insert(v->chars.end(), data.cbegin(), data.cend());
    v->offsets.push_back(v->offsets.back() + data.length());
    return 1;
  }
}


}  // namespace io
}  // namespace ral
