#pragma once

#include "GPUComponentMessage.h"
#include <blazingdb/transport/Message.h>
#include <blazingdb/transport/common/macros.hpp>
#include <rmm/device_buffer.hpp>

namespace ral {
namespace communication {
namespace messages {
namespace experimental {



struct SampleToNodeMasterMessage : GPUComponentMessage {
	SampleToNodeMasterMessage(const std::string & message_token,
		const uint32_t & context_token,
		Node  & sender_node,
		const ral::frame::BlazingTableView & samples, 
		int total_row_size)
		: GPUComponentMessage(message_token, context_token, sender_node, samples,  total_row_size) {}

	DefineClassName(SampleToNodeMasterMessage);

	std::size_t getTotalRowSize() const { return this->metadata().total_row_size; }

	static std::shared_ptr<GPUReceivedMessage> MakeFrom(const Message::MetaData & message_metadata,
		const Address::MetaData & address_metadata,
		const std::vector<ColumnTransport> & columns_offsets,
		const std::vector<rmm::device_buffer> & raw_buffers) {
		return GPUComponentMessage::MakeFrom(message_metadata, address_metadata, columns_offsets, raw_buffers);
	}
};

struct ColumnDataMessage : GPUComponentMessage {
	ColumnDataMessage(const std::string & message_token,
		const uint32_t & context_token,
		Node  & sender_node,
		const ral::frame::BlazingTableView & samples)
		: GPUComponentMessage(message_token, context_token, sender_node, samples) {}

	DefineClassName(ColumnDataMessage);

	// std::unique_ptr<ral::frame::BlazingTableView>& getColumns() { return this->table_view; }

	static std::shared_ptr<GPUReceivedMessage> MakeFrom(const Message::MetaData & message_metadata,
		const Address::MetaData & address_metadata,
		const std::vector<ColumnTransport> & columns_offsets,
		const std::vector<rmm::device_buffer> & raw_buffers) {
		return GPUComponentMessage::MakeFrom(message_metadata, address_metadata, columns_offsets, raw_buffers);
	}
};

struct PartitionPivotsMessage : GPUComponentMessage {
	PartitionPivotsMessage(const std::string & message_token,
		const uint32_t & context_token,
		Node  & sender_node,
		const ral::frame::BlazingTableView & samples)
		: GPUComponentMessage(message_token, context_token, sender_node, samples) {}

	DefineClassName(PartitionPivotsMessage);

	// std::unique_ptr<ral::frame::BlazingTableView>& getColumns() { return this->table_view; }

	static std::shared_ptr<GPUReceivedMessage> MakeFrom(const Message::MetaData & message_metadata,
		const Address::MetaData & address_metadata,
		const std::vector<ColumnTransport> & columns_offsets,
		const std::vector<rmm::device_buffer> & raw_buffers) {
		return GPUComponentMessage::MakeFrom(message_metadata, address_metadata, columns_offsets, raw_buffers);
	}
};

} // namespace experimental
}  // namespace messages
}  // namespace communication
}  // namespace ral