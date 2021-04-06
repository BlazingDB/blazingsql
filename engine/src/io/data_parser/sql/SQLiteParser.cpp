/*
 * Copyright 2021 BlazingDB, Inc.
 *     Copyright 2021 Percy Camilo Triveño Aucahuasi <percy@blazingdb.com>
 */

#include "SQLiteParser.h"
#include "sqlcommon.h"

#include "utilities/CommonOperations.h"

#include <numeric>

#include "ExceptionHandling/BlazingThread.h"
#include <arrow/io/file.h>
#include <sqlite3.h>

#include <parquet/column_writer.h>
#include <parquet/file_writer.h>

#include "Util/StringUtil.h"

namespace ral {
namespace io {

namespace cudf_io = cudf::io;

// TODO percy this is too naive ... improve this later
// TEXT types:
// - CHARACTER(20)
// - VARCHAR(255)
// - VARYING CHARACTER(255)
// - NCHAR(55)
// - NATIVE CHARACTER(70)
// - NVARCHAR(100)
// - TEXT
// - CLOB
bool sqlite_is_cudf_string(const std::string &t) {
  std::vector<std::string> mysql_string_types_hints = {
      "CHARACTER",
      "VARCHAR",
      "VARYING CHARACTER",
      "NCHAR",
      "NATIVE CHARACTER",
      "NVARCHAR",
      "TEXT",
      "CLOB",
      "STRING"  // TODO percy ???
  };

  for (auto hint : mysql_string_types_hints) {
    if (StringUtil::beginsWith(t, hint)) return true;
  }

  return false;
}

cudf::type_id parse_sqlite_column_type(const std::string t) {
  if (sqlite_is_cudf_string(t)) return cudf::type_id::STRING;
  // test numeric data types ...
  if (StringUtil::beginsWith(t, "BOOL") || StringUtil::beginsWith(t, "BOOLEAN"))
    return cudf::type_id::BOOL8;
  if (StringUtil::beginsWith(t, "TINYINT")) return cudf::type_id::INT8;
  if (StringUtil::beginsWith(t, "INT") || StringUtil::beginsWith(t, "INTEGER"))
    return cudf::type_id::INT32;
  if (StringUtil::beginsWith(t, "BIGINT")) return cudf::type_id::INT64;
  if (StringUtil::beginsWith(t, "FLOAT")) return cudf::type_id::FLOAT32;
  if (StringUtil::beginsWith(t, "DOUBLE")) return cudf::type_id::FLOAT64;
  // test date/datetime data types ...
  if (StringUtil::beginsWith(t, "DATE")) return cudf::type_id::TIMESTAMP_DAYS;
  // TODO percy ...
}

std::vector<cudf::type_id>
parse_sqlite_column_types(const std::vector<std::string> types) {
  std::vector<cudf::type_id> ret;
  for (auto t : types) { ret.push_back(parse_sqlite_column_type(t)); }
  return ret;
}

cudf::io::table_with_metadata
read_sqlite_v2(sqlite3_stmt *stmt,
               const std::vector<int> &column_indices,
               const std::vector<cudf::type_id> &cudf_types) {
  const std::string sqlfPart{sqlite3_expanded_sql(stmt)};
  std::string::size_type fPos = sqlfPart.find("from");
  if (fPos == std::string::npos) {
    fPos = sqlfPart.find("FROM");
  }

  std::ostringstream oss;
  oss << "select count(*) " << sqlfPart.substr(fPos);
  const std::string sqlnRows = oss.str();

  std::size_t nRows = 0;
  int err = sqlite3_exec(
      sqlite3_db_handle(stmt),
      sqlnRows.c_str(),
      [](void *data, int count, char **rows, char **) -> int {
        if (count == 1 && rows) {
          *static_cast<std::size_t *>(data) =
              static_cast<std::size_t>(atoi(rows[0]));
          return 0;
        }
        return 1;
      },
      &nRows,
      nullptr);
  if (err != SQLITE_OK) { throw std::runtime_error("getting number of rows"); }

  std::size_t column_count =
      static_cast<std::size_t>(sqlite3_column_count(stmt));

  std::vector<void *> host_cols;
  host_cols.reserve(column_count);
  const std::size_t bitmask_allocation =
      cudf::bitmask_allocation_size_bytes(nRows);
  const std::size_t num_words = bitmask_allocation / sizeof(cudf::bitmask_type);
  std::vector<std::vector<cudf::bitmask_type>> null_masks(column_count);

  std::transform(
      column_indices.cbegin(),
      column_indices.cend(),
      std::back_inserter(host_cols),
      [&cudf_types, &null_masks, num_words, nRows](const int projection_index) {
        null_masks[projection_index].resize(num_words, 0);
        const cudf::type_id cudf_type_id = cudf_types[projection_index];
        switch (cudf_type_id) {
        case cudf::type_id::INT8: {
          auto *vector = new std::vector<std::int8_t>;
          vector->reserve(nRows);
          return static_cast<void *>(vector);
        }
        case cudf::type_id::INT16: {
          auto *vector = new std::vector<std::int16_t>;
          vector->reserve(nRows);
          return static_cast<void *>(vector);
        }
        case cudf::type_id::INT32: {
          auto *vector = new std::vector<std::int32_t>;
          vector->reserve(nRows);
          return static_cast<void *>(vector);
        }
        case cudf::type_id::INT64: {
          auto *vector = new std::vector<std::int64_t>;
          vector->reserve(nRows);
          return static_cast<void *>(vector);
        }
        case cudf::type_id::FLOAT32:
        case cudf::type_id::DECIMAL32: {
          auto *vector = new std::vector<float>;
          vector->reserve(nRows);
          return static_cast<void *>(vector);
        }
        case cudf::type_id::FLOAT64:
        case cudf::type_id::DECIMAL64: {
          auto *vector = new std::vector<double>;
          vector->reserve(nRows);
          return static_cast<void *>(vector);
        }
        case cudf::type_id::BOOL8: {
          auto *vector = new std::vector<std::uint8_t>;
          vector->reserve(nRows);
          return static_cast<void *>(vector);
        }
        default:
          throw std::runtime_error("Invalid allocation for cudf type id");
        }
      });

  std::size_t i = 0;
  while ((err = sqlite3_step(stmt)) == SQLITE_ROW) {
    for (const std::size_t projection_index : column_indices) {
      cudf::type_id cudf_type_id = cudf_types[projection_index];

      const bool isNull =
          sqlite3_column_type(stmt, projection_index) == SQLITE_NULL;

      switch (cudf_type_id) {
      case cudf::type_id::INT8: {
        break;
      }
      case cudf::type_id::INT16: {
        break;
      }
      case cudf::type_id::INT32: {
        const std::int32_t value = sqlite3_column_int(stmt, projection_index);
        std::vector<std::int32_t> &vector =
            *reinterpret_cast<std::vector<std::int32_t> *>(
                host_cols[projection_index]);
        vector.push_back(value);
        break;
      }
      case cudf::type_id::INT64: {
        const std::int64_t value = sqlite3_column_int64(stmt, projection_index);
        std::vector<std::int64_t> &vector =
            *reinterpret_cast<std::vector<std::int64_t> *>(
                host_cols[projection_index]);
        vector.push_back(value);
        break;
      }
      case cudf::type_id::FLOAT32:
      case cudf::type_id::DECIMAL32: {
        break;
      }
      case cudf::type_id::FLOAT64:
      case cudf::type_id::DECIMAL64: {
        const double value = sqlite3_column_double(stmt, projection_index);
        std::vector<double> &vector = *reinterpret_cast<std::vector<double> *>(
            host_cols[projection_index]);
        vector.push_back(value);
        break;
      }
      case cudf::type_id::BOOL8: {
        break;
      }
      case cudf::type_id::STRING: {
        cudf_string_col *string_col =
            reinterpret_cast<cudf_string_col *>(host_cols[projection_index]);
        if (isNull) {
          string_col->offsets.push_back(string_col->offsets.back());
        } else {
          const unsigned char *text =
              sqlite3_column_text(stmt, projection_index);
          const std::string value{reinterpret_cast<const char *>(text)};

          string_col->chars.insert(
              string_col->chars.end(), value.cbegin(), value.cend());
          string_col->offsets.push_back(string_col->offsets.back() +
                                        value.length());
        }
        break;
      }
      default: throw std::runtime_error("Invalid allocation for cudf type id");
      }
      if (isNull) {
        cudf::set_bit_unsafe(null_masks[projection_index].data(), i);
      }
      i++;
    }
  }

  cudf::io::table_with_metadata tableWithMetadata;
  std::vector<std::unique_ptr<cudf::column>> cudf_columns;
  cudf_columns.resize(column_count);
  for (const std::size_t projection_index : column_indices) {
    cudf::type_id cudf_type_id = cudf_types[projection_index];
    switch (cudf_type_id) {
    case cudf::type_id::INT8: {
      std::vector<std::int8_t> *vector =
          reinterpret_cast<std::vector<std::int8_t> *>(
              host_cols[projection_index]);
      cudf_columns[projection_index] = build_fixed_width_cudf_col(
          nRows, vector, null_masks[projection_index], cudf_type_id);
      break;
    }
    case cudf::type_id::INT16: {
      std::vector<std::int16_t> *vector =
          reinterpret_cast<std::vector<std::int16_t> *>(
              host_cols[projection_index]);
      cudf_columns[projection_index] = build_fixed_width_cudf_col(
          nRows, vector, null_masks[projection_index], cudf_type_id);
      break;
    }
    case cudf::type_id::INT32: {
      std::vector<std::int32_t> *vector =
          reinterpret_cast<std::vector<std::int32_t> *>(
              host_cols[projection_index]);
      cudf_columns[projection_index] = build_fixed_width_cudf_col(
          nRows, vector, null_masks[projection_index], cudf_type_id);
      break;
    }
    case cudf::type_id::INT64: {
      std::vector<std::int64_t> *vector =
          reinterpret_cast<std::vector<std::int64_t> *>(
              host_cols[projection_index]);
      cudf_columns[projection_index] = build_fixed_width_cudf_col(
          nRows, vector, null_masks[projection_index], cudf_type_id);
      break;
    }
    case cudf::type_id::FLOAT32:
    case cudf::type_id::DECIMAL32: {
      std::vector<float> *vector =
          reinterpret_cast<std::vector<float> *>(host_cols[projection_index]);
      cudf_columns[projection_index] = build_fixed_width_cudf_col(
          nRows, vector, null_masks[projection_index], cudf_type_id);
      break;
    }
    case cudf::type_id::FLOAT64:
    case cudf::type_id::DECIMAL64: {
      std::vector<double> *vector =
          reinterpret_cast<std::vector<double> *>(host_cols[projection_index]);
      cudf_columns[projection_index] = build_fixed_width_cudf_col(
          nRows, vector, null_masks[projection_index], cudf_type_id);
      break;
    }
    case cudf::type_id::BOOL8: {
      std::vector<std::uint8_t> *vector =
          reinterpret_cast<std::vector<std::uint8_t> *>(
              host_cols[projection_index]);
      cudf_columns[projection_index] = build_fixed_width_cudf_col(
          nRows, vector, null_masks[projection_index], cudf_type_id);
      break;
    }
    case cudf::type_id::STRING: {
      cudf_string_col *string_col =
          reinterpret_cast<cudf_string_col *>(host_cols[projection_index]);
      cudf_columns[projection_index] =
          build_str_cudf_col(string_col, null_masks[projection_index]);
      break;
    }
    default: throw std::runtime_error("Invalid allocation for cudf type id");
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

cudf::io::table_with_metadata
read_sqlite(sqlite3_stmt *stmt, const std::vector<std::string> types) {
  int total_rows = 17;  // TODO percy add this logic to the provider
  cudf::io::table_with_metadata ret;
  std::vector<cudf::type_id> cudf_types = parse_sqlite_column_types(types);
  std::vector<std::vector<char *>> host_cols(types.size());
  std::vector<std::vector<uint32_t>> host_valids(host_cols.size());

  for (int col = 0; col < host_cols.size(); ++col) {
    host_cols[col].resize(total_rows);
  }

  for (int col = 0; col < host_valids.size(); ++col) {
    host_valids[col].resize(total_rows);
  }

  int row = 0;
  int rc = 0;
  while ((rc = sqlite3_step(stmt)) == SQLITE_ROW) {
    for (int col = 0; col < cudf_types.size(); ++col) {
      int mysql_col = col;
      char *value = nullptr;
      size_t data_size = 0;
      cudf::type_id cudf_type_id = cudf_types[col];
      switch (cudf_type_id) {
      case cudf::type_id::EMPTY: {
      } break;
      case cudf::type_id::INT8: {

      } break;
      case cudf::type_id::INT16: {

      } break;
      case cudf::type_id::INT32: {
        int32_t type = sqlite3_column_int(stmt, mysql_col);
        value = (char *) type;
        data_size = sizeof(int32_t);
      } break;
      case cudf::type_id::INT64: {
        std::int32_t type = sqlite3_column_int64(stmt, mysql_col);
        value = (char *) type;
        data_size = sizeof(std::int64_t);
      } break;
      case cudf::type_id::UINT8: {

      } break;
      case cudf::type_id::UINT16: {

      } break;
      case cudf::type_id::UINT32: {

      } break;
      case cudf::type_id::UINT64: {
        int64_t type = sqlite3_column_int64(stmt, mysql_col);
        value = (char *) type;
        data_size = sizeof(int64_t);
      } break;
      case cudf::type_id::FLOAT32: {

      } break;
      case cudf::type_id::FLOAT64: {

      } break;
      case cudf::type_id::BOOL8: {

      } break;
      case cudf::type_id::TIMESTAMP_DAYS: {

      } break;
      case cudf::type_id::TIMESTAMP_SECONDS: {

      } break;
      case cudf::type_id::TIMESTAMP_MILLISECONDS: {

      } break;
      case cudf::type_id::TIMESTAMP_MICROSECONDS: {

      } break;
      case cudf::type_id::TIMESTAMP_NANOSECONDS: {

      } break;
      case cudf::type_id::DURATION_DAYS: {

      } break;
      case cudf::type_id::DURATION_SECONDS: {

      } break;
      case cudf::type_id::DURATION_MILLISECONDS: {

      } break;
      case cudf::type_id::DURATION_MICROSECONDS: {

      } break;
      case cudf::type_id::DURATION_NANOSECONDS: {

      } break;
      case cudf::type_id::DICTIONARY32: {

      } break;
      case cudf::type_id::STRING: {
        const unsigned char *name = sqlite3_column_text(stmt, mysql_col);
        std::string tmpstr((char *) name);
        data_size = tmpstr.size() + 1;  // +1 for null terminating char
        value = (char *) malloc(data_size);
        // value = (char*)tmpstr.c_str();
        strncpy(value, tmpstr.c_str(), data_size);
      } break;
      case cudf::type_id::LIST: {

      } break;
      case cudf::type_id::DECIMAL32: {

      } break;
      case cudf::type_id::DECIMAL64: {

      } break;
      case cudf::type_id::STRUCT: {

      } break;
      }
      host_cols[col][row] = value;
      host_valids[col][row] = (value == nullptr || value == NULL) ? 0 : 1;
      // std::cout << "\t\t" << res->getString("dept_no") << "\n";
    }
    ++row;
  }

  std::vector<std::unique_ptr<cudf::column>> cudf_cols(cudf_types.size());

  for (int col = 0; col < cudf_cols.size(); ++col) {
    switch (cudf_types[col]) {
    case cudf::type_id::EMPTY: {
    } break;
    case cudf::type_id::INT8: {

    } break;
    case cudf::type_id::INT16: {

    } break;
    case cudf::type_id::INT32: {
      int32_t *cols_buff = (int32_t *) host_cols[col].data();
      uint32_t *valids_buff = (uint32_t *) host_valids[col].data();
      std::vector<int32_t> cols(cols_buff, cols_buff + total_rows);
      std::vector<uint32_t> valids(valids_buff, valids_buff + total_rows);
      //cudf::test::fixed_width_column_wrapper<int32_t> vals(
      //cols.begin(), cols.end(), valids.begin());
      //cudf_cols[col] = std::move(vals.release());
    } break;
    case cudf::type_id::INT64: {

    } break;
    case cudf::type_id::UINT8: {

    } break;
    case cudf::type_id::UINT16: {

    } break;
    case cudf::type_id::UINT32: {

    } break;
    case cudf::type_id::UINT64: {

    } break;
    case cudf::type_id::FLOAT32: {

    } break;
    case cudf::type_id::FLOAT64: {

    } break;
    case cudf::type_id::BOOL8: {

    } break;
    case cudf::type_id::TIMESTAMP_DAYS: {
      // TODO percy
      //        int32_t *cols_buff = (int32_t*)host_cols[col].data();
      //        uint32_t *valids_buff = (uint32_t*)host_valids[col].data();
      //        std::vector<int32_t> cols(cols_buff, cols_buff + total_rows);
      //        std::vector<uint32_t> valids(valids_buff, valids_buff +
      //        total_rows); cudf::test::fixed_width_column_wrapper<int32_t>
      //        vals(cols.begin(), cols.end(), valids.begin()); cudf::test::
      //        cudf_cols[col] = std::move(vals.release());
    } break;
    case cudf::type_id::TIMESTAMP_SECONDS: {

    } break;
    case cudf::type_id::TIMESTAMP_MILLISECONDS: {

    } break;
    case cudf::type_id::TIMESTAMP_MICROSECONDS: {

    } break;
    case cudf::type_id::TIMESTAMP_NANOSECONDS: {

    } break;
    case cudf::type_id::DURATION_DAYS: {

    } break;
    case cudf::type_id::DURATION_SECONDS: {

    } break;
    case cudf::type_id::DURATION_MILLISECONDS: {

    } break;
    case cudf::type_id::DURATION_MICROSECONDS: {

    } break;
    case cudf::type_id::DURATION_NANOSECONDS: {

    } break;
    case cudf::type_id::DICTIONARY32: {

    } break;
    case cudf::type_id::STRING: {
      std::vector<std::string> cols(total_rows);
      for (int row_index = 0; row_index < host_cols[col].size(); ++row_index) {
        void *dat = host_cols[col][row_index];
        char *strdat = (char *) dat;
        std::string v(strdat);
        cols[row_index] = v;
      }

      // char **cols_buff = (char**)host_cols[col].data();
      // std::vector<std::string> cols(cols_buff, cols_buff + total_rows);

      uint32_t *valids_buff = (uint32_t *) host_valids[col].data();
      std::vector<uint32_t> valids(valids_buff, valids_buff + total_rows);

      //cudf::test::strings_column_wrapper vals(
      //cols.begin(), cols.end(), valids.begin());
      //cudf_cols[col] = std::move(vals.release());
    } break;
    case cudf::type_id::LIST: {

    } break;
    case cudf::type_id::DECIMAL32: {

    } break;
    case cudf::type_id::DECIMAL64: {

    } break;
    case cudf::type_id::STRUCT: {

    } break;
    }
    // cudf::strings::
    // rmm::device_buffer values(static_cast<void *>(host_cols[col].data()),
    // total_rows); rmm::device_buffer null_mask(static_cast<void
    // *>(host_valids[col].data()), total_rows); cudf::column(cudf_types[col],
    // total_rows, values.data(), null_mask.data());
  }

  // std::unique_ptr<cudf::column> col =
  // cudf::make_empty_column(numeric_column(cudf::data_type(cudf::type_id::INT32),
  // 20);
  // using DecimalTypes = cudf::test::Types<int8_t, int16_t, int32_t, int64_t>;

  // std::vector<int32_t> dat = {5, 4, 3, 5, 8, 5, 6, 5};
  // std::vector<uint32_t> valy = {1, 1, 1, 1, 1, 1, 1, 1};
  // cudf::test::fixed_width_column_wrapper<int32_t> vals(dat.begin(),
  // dat.end(), valy.begin());

  ret.tbl = std::make_unique<cudf::table>(std::move(cudf_cols));
  return ret;
}

sqlite_parser::sqlite_parser() {}

sqlite_parser::~sqlite_parser() {}

std::unique_ptr<ral::frame::BlazingTable>
sqlite_parser::parse_batch(ral::io::data_handle handle,
                           const Schema &schema,
                           std::vector<int> column_indices,
                           std::vector<cudf::size_type> row_groups) {
  auto stmt = handle.sql_handle.sqlite_statement;
  if (stmt == nullptr) { return schema.makeEmptyBlazingTable(column_indices); }

  if (column_indices.size() > 0) {
    std::vector<std::string> col_names(column_indices.size());

    for (size_t column_i = 0; column_i < column_indices.size(); column_i++) {
      col_names[column_i] = schema.get_name(column_indices[column_i]);
    }

    auto result =
        read_sqlite_v2(stmt.get(), column_indices, schema.get_dtypes());
    result.metadata.column_names = col_names;

    auto result_table = std::move(result.tbl);
    if (result.metadata.column_names.size() > column_indices.size()) {
      auto columns = result_table->release();
      // Assuming columns are in the same order as column_indices and any extra
      // columns (i.e. index column) are put last
      columns.resize(column_indices.size());
      result_table = std::make_unique<cudf::table>(std::move(columns));
    }

    return std::make_unique<ral::frame::BlazingTable>(
        std::move(result_table), result.metadata.column_names);
  }
  return nullptr;
}

void sqlite_parser::parse_schema(data_handle handle, Schema &schema) {
  const bool is_in_file = true;
  for (int i = 0; i < handle.sql_handle.column_names.size(); i++) {
    const std::string &column_type = handle.sql_handle.column_types.at(i);
    cudf::type_id type = parse_sqlite_column_type(column_type);
    const std::string &name = handle.sql_handle.column_names.at(i);
    schema.add_column(name, type, i, is_in_file);
  }
}

std::unique_ptr<frame::BlazingTable>
sqlite_parser::get_metadata(std::vector<data_handle> handles, int offset) {
  //	std::vector<size_t> num_row_groups(files.size());
  //	BlazingThread threads[files.size()];
  //	std::vector<std::unique_ptr<parquet::ParquetFileReader>>
  // parquet_readers(files.size()); 	for(size_t file_index = 0; file_index <
  // files.size(); file_index++) { 		threads[file_index] = BlazingThread([&,
  // file_index]() { 		  parquet_readers[file_index] =
  //			  std::move(parquet::ParquetFileReader::Open(files[file_index]));
  //		  std::shared_ptr<parquet::FileMetaData> file_metadata =
  // parquet_readers[file_index]->metadata(); 		  num_row_groups[file_index] =
  // file_metadata->num_row_groups();
  //		});
  //	}

  //	for(size_t file_index = 0; file_index < files.size(); file_index++) {
  //		threads[file_index].join();
  //	}

  //	size_t total_num_row_groups =
  //		std::accumulate(num_row_groups.begin(), num_row_groups.end(),
  // size_t(0));

  //	auto minmax_metadata_table = get_minmax_metadata(parquet_readers,
  // total_num_row_groups, offset); 	for (auto &reader : parquet_readers) {
  //		reader->Close();
  //	}
  //	return minmax_metadata_table;
}

} /* namespace io */
} /* namespace ral */
