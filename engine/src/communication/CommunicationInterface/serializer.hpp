#pragma once

#include <vector>
#include <map>
#include <memory>
#include <tuple>
#include <rmm/device_buffer.hpp>
#include <blazingdb/transport/ColumnTransport.h>
#include "execution_graph/logic_controllers/LogicPrimitives.h"

namespace comm {

using gpu_raw_buffer_container = std::tuple<
  std::vector<std::size_t>,
  std::vector<const char *>,
  std::vector<blazingdb::transport::ColumnTransport>,
  std::vector<std::unique_ptr<rmm::device_buffer>>
  >;

gpu_raw_buffer_container serialize_gpu_message_to_gpu_containers(ral::frame::BlazingTableView table_view);

std::unique_ptr<ral::frame::BlazingTable> deserialize_from_gpu_raw_buffers(
  const std::vector<blazingdb::transport::ColumnTransport> & columns_offsets,
  const std::vector<rmm::device_buffer> & raw_buffers);

} // namespace comm
