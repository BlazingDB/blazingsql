/*
 * Copyright 2021 Percy Camilo Triveño Aucahuasi <percy.camilo.ta@gmail.com>
 */

#ifndef _ABSTRACTSQLPARSER_H_
#define _ABSTRACTSQLPARSER_H_

#include "io/data_parser/DataParser.h"
#include "arrow/io/interfaces.h"
#include <memory>
#include <vector>
#include <cudf/io/types.hpp>

namespace ral {
namespace io {

typedef struct cudf_string_col;

class abstractsql_parser : public data_parser {
public:
	abstractsql_parser(DataType sql_datatype);
	virtual ~abstractsql_parser();

  std::unique_ptr<ral::frame::BlazingTable> parse_batch(
		ral::io::data_handle handle,
		const Schema & schema,
		std::vector<int> column_indices,
		std::vector<cudf::size_type> row_groups) override;

	void parse_schema(ral::io::data_handle handle, Schema & schema) override;

	std::unique_ptr<ral::frame::BlazingTable> get_metadata(
      std::vector<ral::io::data_handle> handles,
      int offset) override;

	DataType type() const override { return this->sql_datatype; }

protected:
  virtual void read_sql_loop(void *src,
      const std::vector<cudf::type_id> &cudf_types,
      const std::vector<int> &column_indices,
      std::vector<void*> &host_cols,
      std::vector<std::vector<cudf::bitmask_type>> &null_masks) = 0;

  virtual cudf::type_id get_cudf_type_id(const std::string &sql_column_type) = 0;

  // returns 0:false if the value was null ... otherwise returns 1:true
  virtual uint8_t parse_cudf_int8(void *src, size_t col, size_t row, std::vector<int8_t> *v) = 0;
  virtual uint8_t parse_cudf_int16(void *src, size_t col, size_t row, std::vector<int16_t> *v) = 0;
  virtual uint8_t parse_cudf_int32(void *src, size_t col, size_t row, std::vector<int32_t> *v) = 0;
  virtual uint8_t parse_cudf_int64(void *src, size_t col, size_t row, std::vector<int64_t> *v) = 0;
  virtual uint8_t parse_cudf_uint8(void *src, size_t col, size_t row, std::vector<uint8_t> *v) = 0;
  virtual uint8_t parse_cudf_uint16(void *src, size_t col, size_t row, std::vector<uint16_t> *v) = 0;
  virtual uint8_t parse_cudf_uint32(void *src, size_t col, size_t row, std::vector<uint32_t> *v ) = 0;
  virtual uint8_t parse_cudf_uint64(void *src, size_t col, size_t row, std::vector<uint64_t> *v) = 0;
  virtual uint8_t parse_cudf_float32(void *src, size_t col, size_t row, std::vector<float> *v) = 0;
  virtual uint8_t parse_cudf_float64(void *src, size_t col, size_t row, std::vector<double> *v) = 0;
  virtual uint8_t parse_cudf_bool8(void *src, size_t col, size_t row, std::vector<int8_t> *v) = 0;
  virtual uint8_t parse_cudf_timestamp_days(void *src, size_t col, size_t row, cudf_string_col *v) = 0;
  virtual uint8_t parse_cudf_timestamp_seconds(void *src, size_t col, size_t row, cudf_string_col *v) = 0;
  virtual uint8_t parse_cudf_timestamp_milliseconds(void *src, size_t col, size_t row, cudf_string_col *v) = 0;
  virtual uint8_t parse_cudf_timestamp_microseconds(void *src, size_t col, size_t row, cudf_string_col *v) = 0;
  virtual uint8_t parse_cudf_timestamp_nanoseconds(void *src, size_t col, size_t row, cudf_string_col *v) = 0;
  // todo percy it seems we don't support this yet
  // case cudf::type_id::duration_days: {} break;
  // case cudf::type_id::duration_seconds: {} break;
  // case cudf::type_id::duration_milliseconds: {} break;
  // case cudf::type_id::duration_microseconds: {} break;
  // case cudf::type_id::duration_nanoseconds: {} break;
  // case cudf::type_id::dictionary32: {} break;
  virtual uint8_t parse_cudf_string(void *src, size_t col, size_t row, cudf_string_col *v) = 0;
  //virtual bool parse_cudf_list(void *src, size_t col, size_t row) = 0;
  //virtual bool parse_cudf_decimal32(void *src, size_t col, size_t row) = 0;
  //virtual bool parse_cudf_decimal64(void *src, size_t col, size_t row) = 0;
  //virtual bool parse_cudf_struct(void *src, size_t col, size_t row) = 0;

protected:
  void parse_sql(void *src,
      const std::vector<int> &column_indices,
      const std::vector<cudf::type_id> &cudf_types,
      size_t row,
      std::vector<void*> &host_cols,
      std::vector<std::vector<cudf::bitmask_type>> &null_masks);

private:
  cudf::io::table_with_metadata read_sql(void *src,
      const std::vector<int> &column_indices,
      const std::vector<cudf::type_id> &cudf_types,
      size_t total_rows);

  std::unique_ptr<ral::frame::BlazingTable> parse_raw_batch(void *src,
      const Schema & schema,
      std::vector<int> column_indices,
      std::vector<cudf::size_type> row_groups,
      size_t row_count);

private:
  DataType sql_datatype;
};

} /* namespace io */
} /* namespace ral */

#endif /* _ABSTRACTSQLPARSER_H_ */
