/*
 * Copyright 2021 Percy Camilo Triveño Aucahuasi <percy.camilo.ta@gmail.com>
 */

#ifndef _MYSQLSQLPARSER_H_
#define _MYSQLSQLPARSER_H_

#include "io/data_parser/sql/AbstractSQLParser.h"

namespace ral {
namespace io {

class mysql_parser : public abstractsql_parser {
public:
	mysql_parser();
	virtual ~mysql_parser();

protected:
  void read_sql_loop(void *src,
      const std::vector<cudf::type_id> &cudf_types,
      const std::vector<int> &column_indices,
      std::vector<void*> &host_cols,
      std::vector<std::vector<cudf::bitmask_type>> &null_masks) override;

  cudf::type_id get_cudf_type_id(const std::string &sql_column_type) override;

  // returns 0:false if the value was null ... otherwise returns 1:true
  uint8_t parse_cudf_int8(void *src, size_t col, size_t row, std::vector<int8_t> *v) override;
  uint8_t parse_cudf_int16(void *src, size_t col, size_t row, std::vector<int16_t> *v) override;
  uint8_t parse_cudf_int32(void *src, size_t col, size_t row, std::vector<int32_t> *v) override;
  uint8_t parse_cudf_int64(void *src, size_t col, size_t row, std::vector<int64_t> *v) override;
  uint8_t parse_cudf_uint8(void *src, size_t col, size_t row, std::vector<uint8_t> *v) override;
  uint8_t parse_cudf_uint16(void *src, size_t col, size_t row, std::vector<uint16_t> *v) override;
  uint8_t parse_cudf_uint32(void *src, size_t col, size_t row, std::vector<uint32_t> *v ) override;
  uint8_t parse_cudf_uint64(void *src, size_t col, size_t row, std::vector<uint64_t> *v) override;
  uint8_t parse_cudf_float32(void *src, size_t col, size_t row, std::vector<float> *v) override;
  uint8_t parse_cudf_float64(void *src, size_t col, size_t row, std::vector<double> *v) override;
  uint8_t parse_cudf_bool8(void *src, size_t col, size_t row, std::vector<int8_t> *v) override;
  uint8_t parse_cudf_timestamp_days(void *src, size_t col, size_t row, cudf_string_col *v) override;
  uint8_t parse_cudf_timestamp_seconds(void *src, size_t col, size_t row, cudf_string_col *v) override;
  uint8_t parse_cudf_timestamp_milliseconds(void *src, size_t col, size_t row, cudf_string_col *v) override;
  uint8_t parse_cudf_timestamp_microseconds(void *src, size_t col, size_t row, cudf_string_col *v) override;
  uint8_t parse_cudf_timestamp_nanoseconds(void *src, size_t col, size_t row, cudf_string_col *v) override;
  //virtual uint8_t parse_cudf_timestamp_seconds(void *src, size_t col, size_t row, cudf_string_col *v) override;
  //virtual uint8_t parse_cudf_timestamp_milliseconds(void *src, size_t col, size_t row, cudf_string_col *v) override;
  //virtual uint8_t parse_cudf_timestamp_microseconds(void *src, size_t col, size_t row, cudf_string_col *v) override;
  //virtual uint8_t parse_cudf_timestamp_nanoseconds(void *src, size_t col, size_t row, cudf_string_col *v) override;
  // todo percy it seems we don't support this yet
  // case cudf::type_id::duration_days: {} break;
  // case cudf::type_id::duration_seconds: {} break;
  // case cudf::type_id::duration_milliseconds: {} break;
  // case cudf::type_id::duration_microseconds: {} break;
  // case cudf::type_id::duration_nanoseconds: {} break;
  // case cudf::type_id::dictionary32: {} break;
  uint8_t parse_cudf_string(void *src, size_t col, size_t row, cudf_string_col *v) override;
  //virtual bool parse_cudf_list(void *src, size_t col, size_t row) override;
  //virtual bool parse_cudf_decimal32(void *src, size_t col, size_t row) override;
  //virtual bool parse_cudf_decimal64(void *src, size_t col, size_t row) override;
  //virtual bool parse_cudf_struct(void *src, size_t col, size_t row) override;
};

} /* namespace io */
} /* namespace ral */

#endif /* _MYSQLSQLPARSER_H_ */
