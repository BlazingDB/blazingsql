/*
 * Copyright 2021 Percy Camilo Triveño Aucahuasi <percy.camilo.ta@gmail.com>
 * Copyright 2021 Cristhian Alberto Gonzales Castillo
 */

#ifndef _POSTGRESQLSQLPARSER_H_
#define _POSTGRESQLSQLPARSER_H_

#include "io/data_parser/sql/AbstractSQLParser.h"

namespace ral {
namespace io {

class postgresql_parser : public abstractsql_parser {
public:
  postgresql_parser();
  virtual ~postgresql_parser();

protected:
  void read_sql_loop(void * src,
    const std::vector<cudf::type_id> & cudf_types,
    const std::vector<int> & column_indices,
    std::vector<void *> & host_cols,
    std::vector<std::vector<cudf::bitmask_type>> & null_masks) override;

  cudf::type_id get_cudf_type_id(const std::string & sql_column_type) override;

  std::uint8_t parse_cudf_int8(
    void *, std::size_t, std::size_t, std::vector<std::int8_t> *) override;
  std::uint8_t parse_cudf_int16(
    void *, std::size_t, std::size_t, std::vector<std::int16_t> *) override;
  std::uint8_t parse_cudf_int32(
    void *, std::size_t, std::size_t, std::vector<std::int32_t> *) override;
  std::uint8_t parse_cudf_int64(
    void *, std::size_t, std::size_t, std::vector<std::int64_t> *) override;
  std::uint8_t parse_cudf_uint8(
    void *, std::size_t, std::size_t, std::vector<std::uint8_t> *) override;
  std::uint8_t parse_cudf_uint16(
    void *, std::size_t, std::size_t, std::vector<std::uint16_t> *) override;
  std::uint8_t parse_cudf_uint32(
    void *, std::size_t, std::size_t, std::vector<std::uint32_t> *) override;
  std::uint8_t parse_cudf_uint64(
    void *, std::size_t, std::size_t, std::vector<std::uint64_t> *) override;
  std::uint8_t parse_cudf_float32(
    void *, std::size_t, std::size_t, std::vector<float> *) override;
  std::uint8_t parse_cudf_float64(
    void *, std::size_t, std::size_t, std::vector<double> *) override;
  std::uint8_t parse_cudf_bool8(
    void *, std::size_t, std::size_t, std::vector<std::int8_t> *) override;
  std::uint8_t parse_cudf_timestamp_days(
    void *, std::size_t, std::size_t, cudf_string_col *) override;
  std::uint8_t parse_cudf_timestamp_seconds(
    void *, std::size_t, std::size_t, cudf_string_col *) override;
  std::uint8_t parse_cudf_timestamp_milliseconds(
    void *, std::size_t, std::size_t, cudf_string_col *) override;
  std::uint8_t parse_cudf_timestamp_microseconds(
    void *, std::size_t, std::size_t, cudf_string_col *) override;
  std::uint8_t parse_cudf_timestamp_nanoseconds(
    void *, std::size_t, std::size_t, cudf_string_col *) override;
  std::uint8_t parse_cudf_string(
    void *, std::size_t, std::size_t, cudf_string_col *) override;
};

} /* namespace io */
} /* namespace ral */

#endif /* _POSTGRESQLSQLPARSER_H_ */
