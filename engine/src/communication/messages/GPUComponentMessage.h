#pragma once

#include <spdlog/spdlog.h>
#include <spdlog/async.h>
#include <spdlog/sinks/basic_file_sink.h>
#include <spdlog/sinks/stdout_color_sinks.h>

#include <blazingdb/transport/Address.h>
#include <blazingdb/transport/ColumnTransport.h>
#include <blazingdb/transport/Message.h>
#include <blazingdb/transport/Node.h>
#include <blazingdb/transport/Server.h>
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
using Address = blazingdb::transport::Address;
using ColumnTransport = blazingdb::transport::ColumnTransport;
using GPUMessage = blazingdb::transport::GPUMessage;
using ReceivedMessage = blazingdb::transport::ReceivedMessage;
using MessageMetadata = blazingdb::transport::Message::MetaData;
using gpu_raw_buffer_container = blazingdb::transport::gpu_raw_buffer_container;

gpu_raw_buffer_container serialize_gpu_message_to_gpu_containers(ral::frame::BlazingTableView table_view);

std::shared_ptr<ReceivedMessage> deserialize_from_gpu(const MessageMetadata& message_metadata,
														 const Address::MetaData & address_metadata,
														 const std::vector<ColumnTransport> & columns_offsets,
														 const std::vector<rmm::device_buffer> & raw_buffers);

std::unique_ptr<ral::frame::BlazingHostTable> serialize_gpu_message_to_host_table(ral::frame::BlazingTableView table_view);

std::unique_ptr<ral::frame::BlazingTable> deserialize_from_cpu(const ral::frame::BlazingHostTable* host_table);


class ReceivedDeviceMessage : public ReceivedMessage {
public:
	ReceivedDeviceMessage(std::string const & messageToken,
						uint32_t contextToken,
						Node  & sender_node,
						std::unique_ptr<ral::frame::BlazingTable> && samples,
						int64_t total_row_size = 0,
						int32_t partition_id = 0)
		: ReceivedMessage(messageToken, contextToken, sender_node),
		  table(std::move(samples)) {
		this->metadata().total_row_size = total_row_size;
		this->metadata().partition_id = partition_id;
	} 
	
	std::unique_ptr<ral::frame::BlazingTable>  releaseBlazingTable() { return std::move(table); }

	int64_t getTotalRowSize() { return this->metadata().total_row_size; };
	
	int32_t getPartitionId() { return this->metadata().partition_id; };

protected:
	std::unique_ptr<ral::frame::BlazingTable> table;
};
// TODO Add ReceivedHostMessage : 
class ReceivedHostMessage : public ReceivedMessage {
public:
	ReceivedHostMessage(std::string const & messageToken,
						uint32_t contextToken,
						Node  & sender_node,
					  std::unique_ptr<ral::frame::BlazingHostTable> samples,
						int64_t total_row_size = 0,
						int32_t partition_id = 0)
		: ReceivedMessage(messageToken, contextToken, sender_node),
		  table(std::move(samples)) {
		this->metadata().total_row_size = total_row_size;
		this->metadata().partition_id = partition_id;
	} 

	std::unique_ptr<ral::frame::BlazingHostTable>  releaseBlazingHostTable() { return std::move(table); }

	std::unique_ptr<ral::frame::BlazingTable>  getBlazingTable() { return deserialize_from_cpu(table.get()); }

	int64_t getTotalRowSize() { return this->metadata().total_row_size; };

	int32_t getPartitionId() { return this->metadata().partition_id; };

protected:
	std::unique_ptr<ral::frame::BlazingHostTable> table;
};

class GPUComponentMessage : public GPUMessage {
public:
	GPUComponentMessage(std::string const & messageToken,
		uint32_t contextToken,
		Node  & sender_node,
		const ral::frame::BlazingTableView & samples,
		int64_t total_row_size = 0,
		int32_t partition_id = 0)
		: GPUMessage(messageToken, contextToken, sender_node), table_view{samples} {
		this->metadata().total_row_size = total_row_size;
		this->metadata().partition_id = partition_id;
	}

	virtual raw_buffer GetRawColumns() override {
		return serialize_gpu_message_to_gpu_containers(table_view);
	}

	static std::shared_ptr<ReceivedMessage> MakeFrom(const Message::MetaData & message_metadata,
		const Address::MetaData & address_metadata,
		const std::vector<ColumnTransport> & columns_offsets,
		const std::vector<rmm::device_buffer> & raw_buffers) {  
		return deserialize_from_gpu(message_metadata, address_metadata, columns_offsets, raw_buffers);
	}

	static std::shared_ptr<ReceivedMessage> MakeFromHost(const Message::MetaData & message_metadata,
		const Address::MetaData & address_metadata,
		const std::vector<ColumnTransport> & columns_offsets,
		std::vector<std::basic_string<char>> && raw_buffers) {  
		auto host_table = std::make_unique<ral::frame::BlazingHostTable>(columns_offsets, std::move(raw_buffers));
		auto node = Node(Address::TCP(address_metadata.ip, address_metadata.comunication_port, address_metadata.protocol_port), "");
		return std::make_shared<ReceivedHostMessage>(message_metadata.messageToken, message_metadata.contextToken, node, std::move(host_table), message_metadata.total_row_size, message_metadata.partition_id);
	}

	ral::frame::BlazingTableView getTableView() { return table_view; }

protected:
	ral::frame::BlazingTableView table_view; 
};

}  // namespace messages
}  // namespace communication
}  // namespace ral
