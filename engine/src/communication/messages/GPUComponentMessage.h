#pragma once

#include <spdlog/spdlog.h>
#include <spdlog/async.h>
#include <spdlog/sinks/basic_file_sink.h>
#include <spdlog/sinks/stdout_color_sinks.h>

#include <blazingdb/transport/ColumnTransport.h>
#include <blazingdb/transport/Node.h>
#include <communication/messages/MessageUtil.cuh>

#include <map>
#include <memory>
#include <mutex>
#include <numeric>
#include <tuple>

#include <cudf/copying.hpp>
#include <cudf/column/column.hpp>
#include <cudf/column/column_view.hpp>
#include <cudf/null_mask.hpp>
#include <cudf/column/column_factories.hpp>
#include <cudf/types.hpp>
#include <cudf/strings/strings_column_view.hpp>

#include <execution_graph/logic_controllers/LogicPrimitives.h>
#include <execution_graph/logic_controllers/BlazingHostTable.h>
namespace ral {
namespace communication {
namespace messages {


using Node = blazingdb::transport::Node;
using ColumnTransport = blazingdb::transport::ColumnTransport;

using gpu_raw_buffer_container = std::tuple<std::vector<std::size_t>, std::vector<const char *>,
											std::vector<ColumnTransport>,
											std::vector<std::unique_ptr<rmm::device_buffer>> >;
gpu_raw_buffer_container serialize_gpu_message_to_gpu_containers(ral::frame::BlazingTableView table_view);


std::unique_ptr<ral::frame::BlazingHostTable> serialize_gpu_message_to_host_table(ral::frame::BlazingTableView table_view);

std::unique_ptr<ral::frame::BlazingTable> deserialize_from_cpu(const ral::frame::BlazingHostTable* host_table);




}  // namespace messages
}  // namespace communication
}  // namespace ral
