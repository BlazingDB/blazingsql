#pragma once

#include <cstdint>

namespace blazingdb {
namespace transport {

struct ColumnTransport {
  struct MetaData {
    int32_t dtype{};
    int32_t size{};
    int32_t null_count{};
    char col_name[128]{};
  };
  MetaData metadata{};
  int data{};  // position del buffer? / (-1) no hay buffer
  int valid{};
  int strings_data{};
  int strings_offsets{};
  int strings_nullmask{};
  int strings_data_size{0};
  int strings_offsets_size{0};

  std::size_t size_in_bytes{0};
};

}  // namespace transport
}  // namespace blazingdb
