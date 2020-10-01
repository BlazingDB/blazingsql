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

/**
 * @brief Serializes a BlazingTableView
 *
 * @param table_view The table to be serialized
 *
 * @returns A tuple containing a vector of buffer sizes, a vector of raw pointers
 * to device column data or intemediate data, a vector of ColumnTransport,
 * a vector of unique_ptr's to device_buffer holding intemediate data not owned
 * by the top level cudf::table.
 */
gpu_raw_buffer_container serialize_gpu_message_to_gpu_containers(ral::frame::BlazingTableView table_view);


/**
 * @brief Deserializes column data and metadata into a BlazingTable
 *
 * @param columns_offsets A vector of ColumnTransport containing column metadata
 * @param raw_buffers A vector of device_buffer containing column data
 *
 * @returns A unique_ptr to BlazingTable created with data from the columns_offsets
 * and raw_buffers vectors.
 */
std::unique_ptr<ral::frame::BlazingTable> deserialize_from_gpu_raw_buffers(
  const std::vector<blazingdb::transport::ColumnTransport> & columns_offsets,
  const std::vector<rmm::device_buffer> & raw_buffers);

} // namespace comm
