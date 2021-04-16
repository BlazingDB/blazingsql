/*
 * Copyright 2021 Percy Camilo Triveño Aucahuasi <percy.camilo.ta@gmail.com>
 */

#include "AbstractSQLParser.h"
#include "sqlcommon.h"

#include <netinet/in.h>

#include <numeric>
#include <arrow/io/file.h>

#include "ExceptionHandling/BlazingThread.h"
#include "Util/StringUtil.h"

#ifdef MYSQL_SUPPORT
#include <mysql/jdbc.h>
#endif

#ifdef SQLITE_SUPPORT
#include <sqlite3.h>
#endif

#ifdef POSTGRESQL_SUPPORT
#include <libpq-fe.h>
#endif

namespace ral {
namespace io {



abstractsql_parser::abstractsql_parser(DataType sql_datatype): sql_datatype(sql_datatype) {
}

abstractsql_parser::~abstractsql_parser() {
}

std::unique_ptr<ral::frame::BlazingTable> abstractsql_parser::parse_batch(
	ral::io::data_handle handle,
	const Schema & schema,
	std::vector<int> column_indices,
	std::vector<cudf::size_type> row_groups)
{
  void *src = nullptr;

#if defined(MYSQL_SUPPORT)
  src = handle.sql_handle.mysql_resultset.get();
#elif defined(POSTGRESQL_SUPPORT)
  src = handle.sql_handle.postgresql_result.get();
#elif defined(SQLITE_SUPPORT)
  src = handle.sql_handle.sqlite_statement.get();
#endif

  return this->parse_raw_batch(src, schema, column_indices, row_groups, handle.sql_handle.row_count);
}

void abstractsql_parser::parse_schema(ral::io::data_handle handle, ral::io::Schema & schema) {
	for(int i = 0; i < handle.sql_handle.column_names.size(); i++) {
		cudf::type_id type = get_cudf_type_id(handle.sql_handle.column_types.at(i));
		size_t file_index = i;
		bool is_in_file = true;
		std::string name = handle.sql_handle.column_names.at(i);
		schema.add_column(name, type, file_index, is_in_file);
	}
}

// TODO percy
std::unique_ptr<ral::frame::BlazingTable> abstractsql_parser::get_metadata(
	std::vector<ral::io::data_handle> handles, int offset){
//	std::vector<size_t> num_row_groups(files.size());
//	BlazingThread threads[files.size()];
//	std::vector<std::unique_ptr<parquet::ParquetFileReader>> parquet_readers(files.size());
//	for(size_t file_index = 0; file_index < files.size(); file_index++) {
//		threads[file_index] = BlazingThread([&, file_index]() {
//		  parquet_readers[file_index] =
//			  std::move(parquet::ParquetFileReader::Open(files[file_index]));
//		  std::shared_ptr<parquet::FileMetaData> file_metadata = parquet_readers[file_index]->metadata();
//		  num_row_groups[file_index] = file_metadata->num_row_groups();
//		});
//	}

//	for(size_t file_index = 0; file_index < files.size(); file_index++) {
//		threads[file_index].join();
//	}

//	size_t total_num_row_groups =
//		std::accumulate(num_row_groups.begin(), num_row_groups.end(), size_t(0));

//	auto minmax_metadata_table = get_minmax_metadata(parquet_readers, total_num_row_groups, offset);
//	for (auto &reader : parquet_readers) {
//		reader->Close();
//	}
//	return minmax_metadata_table;
  return nullptr;
}

void abstractsql_parser::parse_sql(void *src,
  const std::vector<int> &column_indices,
  const std::vector<cudf::type_id> &cudf_types,
  size_t row,
  std::vector<void*> &host_cols,
  std::vector<std::vector<cudf::bitmask_type>> &null_masks)
{
  for (int col = 0; col < column_indices.size(); ++col) {
    size_t projection = column_indices[col];
    uint8_t valid = 1; // 1: not null 0: null
    cudf::type_id cudf_type_id = cudf_types[projection];
    switch (cudf_type_id) {
      case cudf::type_id::EMPTY: {} break;
      case cudf::type_id::INT8: {
        std::vector<int8_t> *v = (std::vector<int8_t>*)host_cols[col];
        valid = this->parse_cudf_int8(src, col, row, v);
      } break;
      case cudf::type_id::INT16: {
        std::vector<int16_t> *v = (std::vector<int16_t>*)host_cols[col];
        valid = this->parse_cudf_int16(src, col, row, v);
      } break;
      case cudf::type_id::INT32: {
        std::vector<int32_t> *v = (std::vector<int32_t>*)host_cols[col];
        valid = this->parse_cudf_int32(src, col, row, v);
      } break;
      case cudf::type_id::INT64: {
        std::vector<int64_t> *v = (std::vector<int64_t>*)host_cols[col];
        valid = this->parse_cudf_int64(src, col, row, v);
      } break;
      case cudf::type_id::UINT8: {
        std::vector<uint8_t> *v = (std::vector<uint8_t>*)host_cols[col];
        valid = this->parse_cudf_uint8(src, col, row, v);
      } break;
      case cudf::type_id::UINT16: {
        std::vector<uint16_t> *v = (std::vector<uint16_t>*)host_cols[col];
        valid = this->parse_cudf_uint16(src, col, row, v);
      } break;
      case cudf::type_id::UINT32: {
        std::vector<uint32_t> *v = (std::vector<uint32_t>*)host_cols[col];
        valid = this->parse_cudf_uint32(src, col, row, v);
      } break;
      case cudf::type_id::UINT64: {
        std::vector<uint64_t> *v = (std::vector<uint64_t>*)host_cols[col];
        valid = this->parse_cudf_uint64(src, col, row, v);
      } break;
      case cudf::type_id::FLOAT32: {
        std::vector<float> *v = (std::vector<float>*)host_cols[col];
        valid = this->parse_cudf_float32(src, col, row, v);
      } break;
      case cudf::type_id::FLOAT64: {
        std::vector<double> *v = (std::vector<double>*)host_cols[col];
        valid = this->parse_cudf_float64(src, col, row, v);
      } break;
      case cudf::type_id::BOOL8: {
        std::vector<int8_t> *v = (std::vector<int8_t>*)host_cols[col];
        valid = this->parse_cudf_bool8(src, col, row, v);
      } break;
      case cudf::type_id::TIMESTAMP_DAYS:
      case cudf::type_id::TIMESTAMP_SECONDS:
      case cudf::type_id::TIMESTAMP_MILLISECONDS:
      case cudf::type_id::TIMESTAMP_MICROSECONDS:
      case cudf::type_id::TIMESTAMP_NANOSECONDS:
      // TODO percy it seems we don't support this yet
      // case cudf::type_id::DURATION_DAYS: {} break;
      // case cudf::type_id::DURATION_SECONDS: {} break;
      // case cudf::type_id::DURATION_MILLISECONDS: {} break;
      // case cudf::type_id::DURATION_MICROSECONDS: {} break;
      // case cudf::type_id::DURATION_NANOSECONDS: {} break;
      // case cudf::type_id::DICTIONARY32: {} break;
      case cudf::type_id::STRING: {
        cudf_string_col *v = (cudf_string_col*)host_cols[col];
        valid = this->parse_cudf_string(src, col, row, v);
      } break;
      // TODO percy
      case cudf::type_id::LIST: {} break;
      case cudf::type_id::DECIMAL32: {} break;
      case cudf::type_id::DECIMAL64: {} break;
      case cudf::type_id::STRUCT: {} break;
      case cudf::type_id::NUM_TYPE_IDS: {} break;
    }

    if (valid) {
      cudf::set_bit_unsafe(null_masks[col].data(), row);
    }
  }
}

std::pair<std::vector<void*>, std::vector<std::vector<cudf::bitmask_type>>> init(
    size_t total_rows, const std::vector<int> &column_indices,
    const std::vector<cudf::type_id> &cudf_types)
{
  cudf::io::table_with_metadata ret;
  std::vector<void*> host_cols(column_indices.size());
  auto const num_words = cudf::bitmask_allocation_size_bytes(total_rows)/sizeof(cudf::bitmask_type);
  auto null_masks = std::vector<std::vector<cudf::bitmask_type>>(host_cols.size());

  for (int col = 0; col < host_cols.size(); ++col) {
    null_masks[col].resize(num_words, 0);
    size_t projection = column_indices[col];
    cudf::type_id cudf_type_id = cudf_types[projection];
    switch (cudf_type_id) {
      case cudf::type_id::EMPTY: {} break;
      case cudf::type_id::INT8: {
        std::vector<int8_t> *tmp = new std::vector<int8_t>();
        tmp->resize(total_rows);
        host_cols[col] = tmp;
      } break;
      case cudf::type_id::INT16: {
        std::vector<int16_t> *tmp = new std::vector<int16_t>();
        tmp->resize(total_rows);
        host_cols[col] = tmp;
      } break;
      case cudf::type_id::INT32: {
        std::vector<int32_t> *tmp = new std::vector<int32_t>();
        tmp->resize(total_rows);
        host_cols[col] = tmp;
      } break;
      case cudf::type_id::INT64: {
        std::vector<int64_t> *tmp = new std::vector<int64_t>();
        tmp->resize(total_rows);
        host_cols[col] = tmp;
      } break;
      case cudf::type_id::UINT8: {
        std::vector<uint8_t> *tmp = new std::vector<uint8_t>();
        tmp->resize(total_rows);
        host_cols[col] = tmp;
      } break;
      case cudf::type_id::UINT16: {
        std::vector<uint16_t> *tmp = new std::vector<uint16_t>();
        tmp->resize(total_rows);
        host_cols[col] = tmp;
      } break;
      case cudf::type_id::UINT32: {
        std::vector<uint32_t> *tmp = new std::vector<uint32_t>();
        tmp->resize(total_rows);
        host_cols[col] = tmp;
      } break;
      case cudf::type_id::UINT64: {
        std::vector<uint64_t> *tmp = new std::vector<uint64_t>();
        tmp->resize(total_rows);
        host_cols[col] = tmp;
      } break;
      case cudf::type_id::FLOAT32: {
        std::vector<float> *tmp = new std::vector<float>();
        tmp->resize(total_rows);
        host_cols[col] = tmp;
      } break;
      case cudf::type_id::FLOAT64: {
        std::vector<double> *tmp = new std::vector<double>();
        tmp->resize(total_rows);
        host_cols[col] = tmp;
      } break;
      case cudf::type_id::BOOL8: {
        std::vector<int8_t> *tmp = new std::vector<int8_t>();
        tmp->resize(total_rows);
        host_cols[col] = tmp;
      } break;
      case cudf::type_id::TIMESTAMP_DAYS:
      case cudf::type_id::TIMESTAMP_SECONDS:
      case cudf::type_id::TIMESTAMP_MILLISECONDS:
      case cudf::type_id::TIMESTAMP_MICROSECONDS:
      case cudf::type_id::TIMESTAMP_NANOSECONDS:
      // TODO percy it seems we don't support this yet
      // case cudf::type_id::DURATION_DAYS: {} break;
      // case cudf::type_id::DURATION_SECONDS: {} break;
      // case cudf::type_id::DURATION_MILLISECONDS: {} break;
      // case cudf::type_id::DURATION_MICROSECONDS: {} break;
      // case cudf::type_id::DURATION_NANOSECONDS: {} break;
      // case cudf::type_id::DICTIONARY32: {} break;
      case cudf::type_id::STRING: {
        auto *tmp = new cudf_string_col();
        tmp->offsets.resize(1, 0);
        host_cols[col] = tmp;
      } break;
      // TODO percy
      case cudf::type_id::LIST: {} break;
      case cudf::type_id::DECIMAL32: {} break;
      case cudf::type_id::DECIMAL64: {} break;
      case cudf::type_id::STRUCT: {} break;
      case cudf::type_id::NUM_TYPE_IDS: {} break;
    }
  }
  
  return std::make_pair(host_cols, null_masks);
}


cudf::io::table_with_metadata abstractsql_parser::read_sql(void *src,
    const std::vector<int> &column_indices,
    const std::vector<cudf::type_id> &cudf_types,
    size_t total_rows)
{
  auto setup = init(total_rows, column_indices, cudf_types);
  std::vector<void*> host_cols = std::move(setup.first);
  auto null_masks = std::move(setup.second);
  this->read_sql_loop(src, cudf_types, column_indices, host_cols, null_masks);
  std::vector<std::unique_ptr<cudf::column>> cudf_cols(host_cols.size());

  for (int col = 0; col < column_indices.size(); ++col) {
    size_t projection = column_indices[col];
    cudf::type_id cudf_type_id = cudf_types[projection];
    switch (cudf_type_id) {
      case cudf::type_id::EMPTY: {} break;
      case cudf::type_id::INT8: {
        std::vector<int8_t> *v = (std::vector<int8_t>*)host_cols[col];
        cudf_cols[col] = build_fixed_width_cudf_col<int8_t>(total_rows, v, null_masks[col], cudf_type_id);
      } break;
      case cudf::type_id::INT16: {
        std::vector<int16_t> *v = (std::vector<int16_t>*)host_cols[col];
        cudf_cols[col] = build_fixed_width_cudf_col<int16_t>(total_rows, v, null_masks[col], cudf_type_id);
      } break;
      case cudf::type_id::INT32: {
        std::vector<int32_t> *v = (std::vector<int32_t>*)host_cols[col];
        cudf_cols[col] = build_fixed_width_cudf_col<int32_t>(total_rows, v, null_masks[col], cudf_type_id);
      } break;
      case cudf::type_id::INT64: {
        std::vector<int64_t> *v = (std::vector<int64_t>*)host_cols[col];
        cudf_cols[col] = build_fixed_width_cudf_col<int64_t>(total_rows, v, null_masks[col], cudf_type_id);
      } break;
      case cudf::type_id::UINT8: {
        std::vector<uint8_t> *v = (std::vector<uint8_t>*)host_cols[col];
        cudf_cols[col] = build_fixed_width_cudf_col<uint8_t>(total_rows, v, null_masks[col], cudf_type_id);
      } break;
      case cudf::type_id::UINT16: {
        std::vector<uint16_t> *v = (std::vector<uint16_t>*)host_cols[col];
        cudf_cols[col] = build_fixed_width_cudf_col<uint16_t>(total_rows, v, null_masks[col], cudf_type_id);
      } break;
      case cudf::type_id::UINT32: {
        std::vector<uint32_t> *v = (std::vector<uint32_t>*)host_cols[col];
        cudf_cols[col] = build_fixed_width_cudf_col<uint32_t>(total_rows, v, null_masks[col], cudf_type_id);
      } break;
      case cudf::type_id::UINT64: {
        std::vector<uint64_t> *v = (std::vector<uint64_t>*)host_cols[col];
        cudf_cols[col] = build_fixed_width_cudf_col<uint64_t>(total_rows, v, null_masks[col], cudf_type_id);
      } break;
      case cudf::type_id::FLOAT32: {
        std::vector<float> *v = (std::vector<float>*)host_cols[col];
        cudf_cols[col] = build_fixed_width_cudf_col<float>(total_rows, v, null_masks[col], cudf_type_id);
      } break;
      case cudf::type_id::FLOAT64: {
        std::vector<double> *v = (std::vector<double>*)host_cols[col];
        cudf_cols[col] = build_fixed_width_cudf_col<double>(total_rows, v, null_masks[col], cudf_type_id);
      } break;
      case cudf::type_id::BOOL8: {
        std::vector<uint8_t> *v = (std::vector<uint8_t>*)host_cols[col];
        cudf_cols[col] = build_fixed_width_cudf_col<uint8_t>(total_rows, v, null_masks[col], cudf_type_id);
      } break;
      case cudf::type_id::TIMESTAMP_DAYS: {
        cudf_string_col *v = (cudf_string_col*)host_cols[col];
        auto str_col = build_str_cudf_col(v, null_masks[col]);
        auto vals_col = cudf::strings::to_timestamps(str_col->view(), cudf::data_type{cudf::type_id::TIMESTAMP_DAYS}, "%Y-%m-%d");
        cudf_cols[col] = std::move(vals_col);
      } break;
      case cudf::type_id::TIMESTAMP_SECONDS: {
        cudf_string_col *v = (cudf_string_col*)host_cols[col];
        auto str_col = build_str_cudf_col(v, null_masks[col]);
        auto vals_col = cudf::strings::to_timestamps(str_col->view(), cudf::data_type{cudf::type_id::TIMESTAMP_SECONDS}, "%H:%M:%S");
        cudf_cols[col] = std::move(vals_col);
      } break;
      case cudf::type_id::TIMESTAMP_MILLISECONDS: {
        cudf_string_col *v = (cudf_string_col*)host_cols[col];
        auto str_col = build_str_cudf_col(v, null_masks[col]);
        auto vals_col = cudf::strings::to_timestamps(str_col->view(), cudf::data_type{cudf::type_id::TIMESTAMP_MILLISECONDS}, "%Y-%m-%d %H:%M:%S");
        cudf_cols[col] = std::move(vals_col);
      } break;
      case cudf::type_id::TIMESTAMP_MICROSECONDS: {} break;
      case cudf::type_id::TIMESTAMP_NANOSECONDS: {} break;
      // TODO percy it seems we don't support this yet
      // case cudf::type_id::DURATION_DAYS: {} break;
      // case cudf::type_id::DURATION_SECONDS: {} break;
      // case cudf::type_id::DURATION_MILLISECONDS: {} break;
      // case cudf::type_id::DURATION_MICROSECONDS: {} break;
      // case cudf::type_id::DURATION_NANOSECONDS: {} break;
      // case cudf::type_id::DICTIONARY32: {} break;
      case cudf::type_id::DICTIONARY32: {} break;
      case cudf::type_id::STRING: {
        cudf_string_col *v = (cudf_string_col*)host_cols[col];
        cudf_cols[col] = build_str_cudf_col(v, null_masks[col]);
      } break;
      case cudf::type_id::LIST: {} break;
      case cudf::type_id::DECIMAL32: {} break;
      case cudf::type_id::DECIMAL64: {} break;
      case cudf::type_id::STRUCT: {} break;
      case cudf::type_id::NUM_TYPE_IDS: {} break;
    }
  }

  cudf::io::table_with_metadata ret;
  ret.tbl = std::make_unique<cudf::table>(std::move(cudf_cols));

  for (int col = 0; col < host_cols.size(); ++col) {
    size_t projection = column_indices[col];
    cudf::type_id cudf_type_id = cudf_types[projection];
    switch (cudf_type_id) {
      case cudf::type_id::EMPTY: {} break;
      case cudf::type_id::INT8: {
        std::vector<int8_t> *tmp = (std::vector<int8_t>*)host_cols[col];
        delete(tmp);
      } break;
      case cudf::type_id::INT16: {
        std::vector<int16_t> *tmp = (std::vector<int16_t>*)host_cols[col];
        delete(tmp);
      } break;
      case cudf::type_id::INT32: {
        std::vector<int32_t> *tmp = (std::vector<int32_t>*)host_cols[col];
        delete(tmp);
      } break;
      case cudf::type_id::INT64: {
        std::vector<int64_t> *tmp = (std::vector<int64_t>*)host_cols[col];
        delete(tmp);
      } break;
      case cudf::type_id::UINT8: {
        std::vector<uint8_t> *tmp = (std::vector<uint8_t>*)host_cols[col];
        delete(tmp);
      } break;
      case cudf::type_id::UINT16: {
        std::vector<uint16_t> *tmp = (std::vector<uint16_t>*)host_cols[col];
        delete(tmp);
      } break;
      case cudf::type_id::UINT32: {
        std::vector<uint32_t> *tmp = (std::vector<uint32_t>*)host_cols[col];
        delete(tmp);
      } break;
      case cudf::type_id::UINT64: {
        std::vector<uint64_t> *tmp = (std::vector<uint64_t>*)host_cols[col];
        delete(tmp);
      } break;
      case cudf::type_id::FLOAT32: {
        std::vector<float> *tmp = (std::vector<float>*)host_cols[col];
        delete(tmp);
      } break;
      case cudf::type_id::FLOAT64: {
        std::vector<double> *tmp = (std::vector<double>*)host_cols[col];
        delete(tmp);
      } break;
      case cudf::type_id::BOOL8: {
        std::vector<int8_t> *tmp = (std::vector<int8_t>*)host_cols[col];
        delete(tmp);
      } break;
      case cudf::type_id::TIMESTAMP_DAYS:
      case cudf::type_id::TIMESTAMP_SECONDS:
      case cudf::type_id::TIMESTAMP_MILLISECONDS:
      case cudf::type_id::TIMESTAMP_MICROSECONDS:
      case cudf::type_id::TIMESTAMP_NANOSECONDS:
      // TODO percy it seems we don't support this yet
      // case cudf::type_id::DURATION_DAYS: {} break;
      // case cudf::type_id::DURATION_SECONDS: {} break;
      // case cudf::type_id::DURATION_MILLISECONDS: {} break;
      // case cudf::type_id::DURATION_MICROSECONDS: {} break;
      // case cudf::type_id::DURATION_NANOSECONDS: {} break;
      // case cudf::type_id::DICTIONARY32: {} break;
      case cudf::type_id::STRING: {
        cudf_string_col *tmp = (cudf_string_col*)host_cols[col];
        delete(tmp);
      } break;
      // TODO percy
      case cudf::type_id::LIST: {} break;
      case cudf::type_id::DECIMAL32: {} break;
      case cudf::type_id::DECIMAL64: {} break;
      case cudf::type_id::STRUCT: {} break;
      case cudf::type_id::NUM_TYPE_IDS: {} break;
    }
  }

  return ret;
}

std::unique_ptr<ral::frame::BlazingTable> abstractsql_parser::parse_raw_batch(
	void *src,
	const Schema & schema,
	std::vector<int> column_indices,
	std::vector<cudf::size_type> row_groups,
  size_t row_count)
{
  // DEBUG
  //std::cout << "PARSING BATCH: " << handle.sql_handle.row_count << "\n";

  if (src == nullptr) { return schema.makeEmptyBlazingTable(column_indices); }

	if(column_indices.size() > 0) {
		std::vector<std::string> col_names(column_indices.size());

		for(size_t column_i = 0; column_i < column_indices.size(); column_i++) {
			col_names[column_i] = schema.get_name(column_indices[column_i]);
		}

		auto result = read_sql(src, column_indices, schema.get_dtypes(), row_count);
    result.metadata.column_names = col_names;

		auto result_table = std::move(result.tbl);
		if (result.metadata.column_names.size() > column_indices.size()) {
			auto columns = result_table->release();
			// Assuming columns are in the same order as column_indices and any extra columns (i.e. index column) are put last
			columns.resize(column_indices.size());
			result_table = std::make_unique<cudf::table>(std::move(columns));
		}

		return std::make_unique<ral::frame::BlazingTable>(std::move(result_table), result.metadata.column_names);
	}

	return nullptr;
}

} /* namespace io */
} /* namespace ral */
