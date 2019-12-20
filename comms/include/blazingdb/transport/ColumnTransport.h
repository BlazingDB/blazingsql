#pragma once
#include <cstdint>

#include <cudf/types.hpp>

namespace blazingdb {
namespace transport {

struct ColumnTransport {
  struct MetaData {
    cudf::data_type dtype{};
    cudf::size_type size{};
    cudf::size_type null_count{};
    int32_t time_unit{};
    char col_name[128]{};
  };
  MetaData metadata{};
  int data{};  // position del buffer? / (-1) no hay buffer
  int valid{};
  int strings_data{};
  int strings_offsets{};
  int strings_nullmask{};
};

}  // namespace transport
}  // namespace blazingdb