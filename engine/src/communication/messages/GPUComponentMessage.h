#pragma once

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
#include "Traits/RuntimeTraits.h"
namespace ral {
namespace communication {
namespace messages {

namespace experimental {

using Node = blazingdb::transport::experimental::Node;
using Address = blazingdb::transport::experimental::Address;
using ColumnTransport = blazingdb::transport::experimental::ColumnTransport;
using GPUMessage = blazingdb::transport::experimental::GPUMessage;
using GPUReceivedMessage = blazingdb::transport::experimental::GPUReceivedMessage;
using MessageMetadata = blazingdb::transport::experimental::Message::MetaData;
using gpu_raw_buffer_container = blazingdb::transport::experimental::gpu_raw_buffer_container;
using HostBufferContainer = blazingdb::transport::experimental::HostBufferContainer;

gpu_raw_buffer_container serialize_gpu_message_to_gpu_containers(ral::frame::BlazingTableView table_view);

std::shared_ptr<GPUReceivedMessage> deserialize_from_gpu(const MessageMetadata& message_metadata,
														 const Address::MetaData & address_metadata,
														 const std::vector<ColumnTransport> & columns_offsets,
														 const std::vector<rmm::device_buffer> & raw_buffers);

ral::frame::BlazingHostTable serialize_gpu_message_to_host_table(ral::frame::BlazingTableView table_view);

std::unique_ptr<ral::frame::BlazingTable> deserialize_from_cpu(const ral::frame::BlazingHostTable& host_table);


class GPUComponentReceivedMessage : public GPUReceivedMessage {
public:
	GPUComponentReceivedMessage(std::string const & messageToken,
						uint32_t contextToken,
						Node  & sender_node,
					    std::unique_ptr<ral::frame::BlazingTable> && samples,
						std::uint64_t total_row_size = 0)
		: GPUReceivedMessage(messageToken, contextToken, sender_node),
		  table(std::move(samples)) {
		this->metadata().total_row_size = total_row_size;
	} 
	
	std::unique_ptr<ral::frame::BlazingTable>  releaseBlazingTable() { return std::move(table); }

	int32_t getTotalRowSize() { return this->metadata().total_row_size; };

protected:
	std::unique_ptr<ral::frame::BlazingTable> table;
};
// TODO Add HostComponentReceivedMessage : 
class HostComponentReceivedMessage : public GPUReceivedMessage {
public:
	HostComponentReceivedMessage(std::string const & messageToken,
						uint32_t contextToken,
						Node  & sender_node,
					    ral::frame::BlazingHostTable && samples,
						std::uint64_t total_row_size = 0)
		: GPUReceivedMessage(messageToken, contextToken, sender_node),
		  table(std::move(samples)) {
		this->metadata().total_row_size = total_row_size;
	} 
	
	std::unique_ptr<ral::frame::BlazingTable>  getBlazingTable() { return deserialize_from_cpu(table); }

	int32_t getTotalRowSize() { return this->metadata().total_row_size; };

protected:
	ral::frame::BlazingHostTable table;
};

class GPUComponentMessage : public GPUMessage {
public:
	GPUComponentMessage(std::string const & messageToken,
		uint32_t contextToken,
		Node  & sender_node,
		const ral::frame::BlazingTableView & samples,
		std::uint64_t total_row_size = 0)
		: GPUMessage(messageToken, contextToken, sender_node), table_view{samples} {
		this->metadata().total_row_size = total_row_size;
	}

	virtual raw_buffer GetRawColumns() override {
		return serialize_gpu_message_to_gpu_containers(table_view);
	}

	static std::shared_ptr<GPUReceivedMessage> MakeFrom(const Message::MetaData & message_metadata,
		const Address::MetaData & address_metadata,
		const std::vector<ColumnTransport> & columns_offsets,
		const std::vector<rmm::device_buffer> & raw_buffers) {  
		return deserialize_from_gpu(message_metadata, address_metadata, columns_offsets, raw_buffers);
	}

	static std::shared_ptr<GPUReceivedMessage> MakeFromHost(const Message::MetaData & message_metadata,
		const Address::MetaData & address_metadata,
		const std::vector<ColumnTransport> & columns_offsets,
		const std::vector<std::basic_string<char>> & raw_buffers) {  
		auto host_table = ral::frame::BlazingHostTable{
			.columns_offsets = columns_offsets, 
			.raw_buffers = raw_buffers
		};
		auto node = Node(Address::TCP(address_metadata.ip, address_metadata.comunication_port, address_metadata.protocol_port));
		return std::make_shared<HostComponentReceivedMessage>(message_metadata.messageToken, message_metadata.contextToken, node, std::move(host_table), message_metadata.total_row_size);
	}

	ral::frame::BlazingTableView getTableView() { return table_view; }

protected:
	ral::frame::BlazingTableView table_view; 
};

}  // namespace experimental
}  // namespace messages
}  // namespace communication
}  // namespace ral