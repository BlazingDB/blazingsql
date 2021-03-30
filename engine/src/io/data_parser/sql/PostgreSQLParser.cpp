/*
 * Copyright 2021 BlazingDB, Inc.
 *     Copyright 2021 Cristhian Alberto Gonzales Castillo
 * <cristhian@blazingdb.com>
 */

#include <array>

#include <cudf/io/types.hpp>
#include <cudf/utilities/bit.hpp>
#include <libpq-fe.h>
#include <netinet/in.h>

#include "PostgreSQLParser.h"

namespace ral {
namespace io {

static const std::array<const char *, 6> postgresql_string_type_hints = {
    "character", "character varying", "bytea", "text", "anyarray", "name"};

static inline bool postgresql_is_cudf_string(const std::string &hint) {
  const auto *result =
      std::find_if(std::cbegin(postgresql_string_type_hints),
                   std::cend(postgresql_string_type_hints),
                   [&hint](const char *c_hint) {
                     return std::strcmp(c_hint, hint.c_str()) == 0;
                   });
  return result != std::cend(postgresql_string_type_hints);
}

template <class T>
static inline std::unique_ptr<cudf::column>
build_fixed_width_cudf_col(const std::size_t total_rows,
                           const std::vector<T> &host_col,
                           const std::vector<cudf::bitmask_type> &null_mask,
                           cudf::type_id cudf_type_id) {
  const cudf::size_type size = total_rows;
  auto data = rmm::device_buffer{host_col.data(), size * sizeof(T)};
  auto data_type = cudf::data_type(cudf_type_id);
  auto null_mask_buff = rmm::device_buffer{
      null_mask.data(), null_mask.size() * sizeof(decltype(null_mask.front()))};
  auto ret = std::make_unique<cudf::column>(
      data_type, size, data, null_mask_buff, cudf::UNKNOWN_NULL_COUNT);
  return ret;
}

static inline cudf::io::table_with_metadata
read_postgresql(const std::shared_ptr<PGresult> &pgResult,
                const std::vector<int> &column_indices,
                const std::vector<cudf::type_id> &cudf_types,
                const std::vector<std::size_t> &column_bytes,
                std::size_t total_rows) {
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
  const std::size_t num_words =
      cudf::bitmask_allocation_size_bytes(resultNtuples) /
      sizeof(cudf::bitmask_type);
  std::vector<std::vector<cudf::bitmask_type>> null_masks(resultNfields);
  std::transform(
      column_indices.cbegin(),
      column_indices.cend(),
      std::back_inserter(host_cols),
      [&pgResult, &cudf_types, &null_masks, num_words, resultNtuples](
          const int projection_index) {
        null_masks[projection_index].resize(num_words, 0);
        const int fsize = PQfsize(pgResult.get(), projection_index);
        if (fsize < 0) {  // STRING, STRUCT, LIST, and similar cases
          auto *vector = new std::vector<std::string>;
          vector->reserve(resultNtuples);
          return static_cast<void *>(vector);
        }
        // primitives cases
        const cudf::type_id cudf_type_id = cudf_types[projection_index];
        switch (cudf_type_id) {
        case cudf::type_id::INT8: {
          auto *vector = new std::vector<std::int8_t>;
          vector->reserve(resultNtuples);
          return static_cast<void *>(vector);
        }
        case cudf::type_id::INT16: {
          auto *vector = new std::vector<std::int16_t>;
          vector->reserve(resultNtuples);
          return static_cast<void *>(vector);
        }
        case cudf::type_id::INT32: {
          auto *vector = new std::vector<std::int32_t>;
          vector->reserve(resultNtuples);
          return static_cast<void *>(vector);
        }
        case cudf::type_id::INT64: {
          auto *vector = new std::vector<std::int64_t>;
          vector->reserve(resultNtuples);
          return static_cast<void *>(vector);
        }
        case cudf::type_id::UINT8: {
          auto *vector = new std::vector<std::uint8_t>;
          vector->reserve(resultNtuples);
          return static_cast<void *>(vector);
        }
        case cudf::type_id::UINT16: {
          auto *vector = new std::vector<std::uint16_t>;
          vector->reserve(resultNtuples);
          return static_cast<void *>(vector);
        }
        case cudf::type_id::UINT32: {
          auto *vector = new std::vector<std::uint32_t>;
          vector->reserve(resultNtuples);
          return static_cast<void *>(vector);
        }
        case cudf::type_id::UINT64: {
          auto *vector = new std::vector<std::uint64_t>;
          vector->reserve(resultNtuples);
          return static_cast<void *>(vector);
        }
        case cudf::type_id::FLOAT32:
        case cudf::type_id::DECIMAL32: {
          auto *vector = new std::vector<float>;
          vector->reserve(resultNtuples);
          return static_cast<void *>(vector);
        }
        case cudf::type_id::FLOAT64:
        case cudf::type_id::DECIMAL64: {
          auto *vector = new std::vector<double>;
          vector->reserve(resultNtuples);
          return static_cast<void *>(vector);
        }
        case cudf::type_id::BOOL8: {
          auto *vector = new std::vector<std::uint8_t>;
          vector->reserve(resultNtuples);
          return static_cast<void *>(vector);
        }
        default:
          throw std::runtime_error("Invalid allocation for cudf type id");
        }
      });

  for (int i = 0; i < resultNtuples; i++) {
    for (const std::size_t projection_index : column_indices) {
      cudf::type_id cudf_type_id = cudf_types[projection_index];
      const char *resultValue = PQgetvalue(pgResult.get(), i, projection_index);
      const bool isNull =
          static_cast<bool>(PQgetisnull(pgResult.get(), i, projection_index));
      switch (cudf_type_id) {
      case cudf::type_id::INT8: {
        const std::int8_t castedValue =
            *reinterpret_cast<const std::int8_t *>(resultValue);
        std::vector<std::int8_t> &vector =
            *reinterpret_cast<std::vector<std::int8_t> *>(
                host_cols[projection_index]);
        vector.push_back(castedValue);
        break;
      }
      case cudf::type_id::INT16: {
        const std::int16_t castedValue =
            *reinterpret_cast<const std::int16_t *>(resultValue);
        const std::int16_t hostOrderedValue = ntohs(castedValue);
        std::vector<std::int16_t> &vector =
            *reinterpret_cast<std::vector<std::int16_t> *>(
                host_cols[projection_index]);
        vector.push_back(hostOrderedValue);
        break;
      }
      case cudf::type_id::INT32: {
        const std::int32_t castedValue =
            *reinterpret_cast<const std::int32_t *>(resultValue);
        const std::int32_t hostOrderedValue = ntohl(castedValue);
        std::vector<std::int32_t> &vector =
            *reinterpret_cast<std::vector<std::int32_t> *>(
                host_cols[projection_index]);
        vector.push_back(hostOrderedValue);
        break;
      }
      case cudf::type_id::INT64: {
        const std::int64_t castedValue =
            *reinterpret_cast<const std::int64_t *>(resultValue);
        const std::int64_t hostOrderedValue = ntohl(castedValue);
        std::vector<std::int64_t> &vector =
            *reinterpret_cast<std::vector<std::int64_t> *>(
                host_cols[projection_index]);
        vector.push_back(hostOrderedValue);
        break;
      }
      case cudf::type_id::UINT8: {
        const std::int8_t castedValue =
            *reinterpret_cast<const std::int8_t *>(resultValue);
        std::vector<std::uint8_t> &vector =
            *reinterpret_cast<std::vector<std::uint8_t> *>(
                host_cols[projection_index]);
        vector.push_back(castedValue);
        break;
      }
      case cudf::type_id::UINT16: {
        const std::int16_t castedValue =
            *reinterpret_cast<const std::int16_t *>(resultValue);
        const std::int16_t hostOrderedValue = ntohs(castedValue);
        std::vector<std::uint16_t> &vector =
            *reinterpret_cast<std::vector<std::uint16_t> *>(
                host_cols[projection_index]);
        vector.push_back(hostOrderedValue);
        break;
      }
      case cudf::type_id::UINT32: {
        const std::int32_t castedValue =
            *reinterpret_cast<const std::int32_t *>(resultValue);
        const std::int32_t hostOrderedValue = ntohl(castedValue);
        std::vector<std::uint32_t> &vector =
            *reinterpret_cast<std::vector<std::uint32_t> *>(
                host_cols[projection_index]);
        vector.push_back(hostOrderedValue);
        break;
      }
      case cudf::type_id::UINT64: {
        const std::int64_t castedValue =
            *reinterpret_cast<const std::int64_t *>(resultValue);
        const std::int64_t hostOrderedValue = ntohl(castedValue);
        std::vector<std::uint64_t> &vector =
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
        std::vector<float> &vector = *reinterpret_cast<std::vector<float> *>(
            host_cols[projection_index]);
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
        std::vector<double> &vector = *reinterpret_cast<std::vector<double> *>(
            host_cols[projection_index]);
        vector.push_back(doubleCastedValue);
        break;
      }
      case cudf::type_id::BOOL8: {
        const std::uint8_t castedValue =
            *reinterpret_cast<const std::uint8_t *>(resultValue);
        std::vector<std::uint8_t> &vector =
            *reinterpret_cast<std::vector<std::uint8_t> *>(
                host_cols[projection_index]);
        vector.push_back(castedValue);
        break;
      }
      case cudf::type_id::STRING: {
        std::vector<std::string> &vector =
            *reinterpret_cast<std::vector<std::string> *>(
                host_cols[projection_index]);
        vector.emplace_back(resultValue);
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
      std::vector<std::int8_t> &vector =
          *reinterpret_cast<std::vector<std::int8_t> *>(
              host_cols[projection_index]);
      cudf_columns[projection_index] = build_fixed_width_cudf_col(
          resultNtuples, vector, null_masks[projection_index], cudf_type_id);
      break;
    }
    case cudf::type_id::INT16: {
      std::vector<std::int16_t> &vector =
          *reinterpret_cast<std::vector<std::int16_t> *>(
              host_cols[projection_index]);
      cudf_columns[projection_index] = build_fixed_width_cudf_col(
          resultNtuples, vector, null_masks[projection_index], cudf_type_id);
      break;
    }
    case cudf::type_id::INT32: {
      std::vector<std::int32_t> &vector =
          *reinterpret_cast<std::vector<std::int32_t> *>(
              host_cols[projection_index]);
      cudf_columns[projection_index] = build_fixed_width_cudf_col(
          resultNtuples, vector, null_masks[projection_index], cudf_type_id);
      break;
    }
    case cudf::type_id::INT64: {
      std::vector<std::int64_t> &vector =
          *reinterpret_cast<std::vector<std::int64_t> *>(
              host_cols[projection_index]);
      cudf_columns[projection_index] = build_fixed_width_cudf_col(
          resultNtuples, vector, null_masks[projection_index], cudf_type_id);
      break;
    }
    case cudf::type_id::UINT8: {
      std::vector<std::uint8_t> &vector =
          *reinterpret_cast<std::vector<std::uint8_t> *>(
              host_cols[projection_index]);
      cudf_columns[projection_index] = build_fixed_width_cudf_col(
          resultNtuples, vector, null_masks[projection_index], cudf_type_id);
      break;
    }
    case cudf::type_id::UINT16: {
      std::vector<std::uint16_t> &vector =
          *reinterpret_cast<std::vector<std::uint16_t> *>(
              host_cols[projection_index]);
      cudf_columns[projection_index] = build_fixed_width_cudf_col(
          resultNtuples, vector, null_masks[projection_index], cudf_type_id);
      break;
    }
    case cudf::type_id::UINT32: {
      std::vector<std::uint32_t> &vector =
          *reinterpret_cast<std::vector<std::uint32_t> *>(
              host_cols[projection_index]);
      cudf_columns[projection_index] = build_fixed_width_cudf_col(
          resultNtuples, vector, null_masks[projection_index], cudf_type_id);
      break;
    }
    case cudf::type_id::UINT64: {
      std::vector<std::uint64_t> &vector =
          *reinterpret_cast<std::vector<std::uint64_t> *>(
              host_cols[projection_index]);
      cudf_columns[projection_index] = build_fixed_width_cudf_col(
          resultNtuples, vector, null_masks[projection_index], cudf_type_id);
      break;
    }
    case cudf::type_id::FLOAT32:
    case cudf::type_id::DECIMAL32: {
      std::vector<float> &vector =
          *reinterpret_cast<std::vector<float> *>(host_cols[projection_index]);
      cudf_columns[projection_index] = build_fixed_width_cudf_col(
          resultNtuples, vector, null_masks[projection_index], cudf_type_id);
      break;
    }
    case cudf::type_id::FLOAT64:
    case cudf::type_id::DECIMAL64: {
      std::vector<double> &vector =
          *reinterpret_cast<std::vector<double> *>(host_cols[projection_index]);
      cudf_columns[projection_index] = build_fixed_width_cudf_col(
          resultNtuples, vector, null_masks[projection_index], cudf_type_id);
      break;
    }
    case cudf::type_id::BOOL8: {
      std::vector<std::uint8_t> &vector =
          *reinterpret_cast<std::vector<std::uint8_t> *>(
              host_cols[projection_index]);
      cudf_columns[projection_index] = build_fixed_width_cudf_col(
          resultNtuples, vector, null_masks[projection_index], cudf_type_id);
      break;
    }
    case cudf::type_id::STRING: {
      std::vector<std::string> &vector =
          *reinterpret_cast<std::vector<std::string> *>(
              host_cols[projection_index]);
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
      delete reinterpret_cast<std::vector<std::string> *>(
          host_cols[projection_index]);
      break;
    }
    default: throw std::runtime_error("Invalid cudf type id");
    }
  }
  return tableWithMetadata;
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

postgresql_parser::postgresql_parser() = default;

postgresql_parser::~postgresql_parser() = default;

std::unique_ptr<frame::BlazingTable>
postgresql_parser::parse_batch(data_handle handle,
                               const Schema &schema,
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
                                             handle.sql_handle.column_bytes,
                                             handle.sql_handle.row_count);
    tableWithMetadata.metadata.column_names = columnNames;

    auto table = std::move(tableWithMetadata.tbl);
    return std::make_unique<frame::BlazingTable>(
        std::move(table), tableWithMetadata.metadata.column_names);
  }

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
