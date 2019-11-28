#pragma once
#include <cstdint>

namespace blazingdb {
namespace transport {

struct ColumnTransport {
  struct MetaData {
    int32_t dtype{};
    int32_t size{};
    int32_t null_count{};
    int32_t time_unit{};
    char col_name[128]{};
  };
  MetaData metadata{};
  int data{};         // position del buffer? / (-1) no hay buffer
  int valid{};
  int strings_data{};
  int strings_offsets{};
  int strings_nullmask{};
};

} // namespace transport
} // namespace blazingdb