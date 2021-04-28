/**
 * Copyright 2021 Cristhian Alberto Gonzales Castillo <gcca.lib@gmail.com>
 */

#include "SnowFlakeParser.h"
#include "sqlcommon.h"

namespace ral {
namespace io {

snowflake_parser::snowflake_parser()
    : abstractsql_parser{DataType::SNOWFLAKE} {}

snowflake_parser::~snowflake_parser() = default;

void snowflake_parser::read_sql_loop(
    void * src,
    const std::vector<cudf::type_id> & cudf_types,
    const std::vector<int> & column_indices,
    std::vector<void *> & host_cols,
    std::vector<std::vector<cudf::bitmask_type>> & null_masks) {}

cudf::type_id
snowflake_parser::get_cudf_type_id(const std::string & sql_column_type) {
  return cudf::type_id::EMPTY;
}

std::uint8_t snowflake_parser::parse_cudf_int8(void *,
                                               std::size_t,
                                               std::size_t,
                                               std::vector<std::int8_t> *) {
  return 0;
}
std::uint8_t snowflake_parser::parse_cudf_int16(void *,
                                                std::size_t,
                                                std::size_t,
                                                std::vector<std::int16_t> *) {
  return 0;
}
std::uint8_t snowflake_parser::parse_cudf_int32(void *,
                                                std::size_t,
                                                std::size_t,
                                                std::vector<std::int32_t> *) {
  return 0;
}
std::uint8_t snowflake_parser::parse_cudf_int64(void *,
                                                std::size_t,
                                                std::size_t,
                                                std::vector<std::int64_t> *) {
  return 0;
}
std::uint8_t snowflake_parser::parse_cudf_uint8(void *,
                                                std::size_t,
                                                std::size_t,
                                                std::vector<std::uint8_t> *) {
  return 0;
}
std::uint8_t snowflake_parser::parse_cudf_uint16(void *,
                                                 std::size_t,
                                                 std::size_t,
                                                 std::vector<std::uint16_t> *) {
  return 0;
}
std::uint8_t snowflake_parser::parse_cudf_uint32(void *,
                                                 std::size_t,
                                                 std::size_t,
                                                 std::vector<std::uint32_t> *) {
  return 0;
}
std::uint8_t snowflake_parser::parse_cudf_uint64(void *,
                                                 std::size_t,
                                                 std::size_t,
                                                 std::vector<std::uint64_t> *) {
  return 0;
}
std::uint8_t snowflake_parser::parse_cudf_float32(void *,
                                                  std::size_t,
                                                  std::size_t,
                                                  std::vector<float> *) {
  return 0;
}
std::uint8_t snowflake_parser::parse_cudf_float64(void *,
                                                  std::size_t,
                                                  std::size_t,
                                                  std::vector<double> *) {
  return 0;
}
std::uint8_t snowflake_parser::parse_cudf_bool8(void *,
                                                std::size_t,
                                                std::size_t,
                                                std::vector<std::int8_t> *) {
  return 0;
}
std::uint8_t snowflake_parser::parse_cudf_timestamp_days(void *,
                                                         std::size_t,
                                                         std::size_t,
                                                         cudf_string_col *) {
  return 0;
}
std::uint8_t snowflake_parser::parse_cudf_timestamp_seconds(void *,
                                                            std::size_t,
                                                            std::size_t,
                                                            cudf_string_col *) {
  return 0;
}
std::uint8_t
snowflake_parser::parse_cudf_timestamp_milliseconds(void *,
                                                    std::size_t,
                                                    std::size_t,
                                                    cudf_string_col *) {
  return 0;
}
std::uint8_t
snowflake_parser::parse_cudf_timestamp_microseconds(void *,
                                                    std::size_t,
                                                    std::size_t,
                                                    cudf_string_col *) {
  return 0;
}
std::uint8_t
snowflake_parser::parse_cudf_timestamp_nanoseconds(void *,
                                                   std::size_t,
                                                   std::size_t,
                                                   cudf_string_col *) {
  return 0;
}
std::uint8_t snowflake_parser::parse_cudf_string(void *,
                                                 std::size_t,
                                                 std::size_t,
                                                 cudf_string_col *) {
  return 0;
}


}  // namespace io
}  // namespace ral
