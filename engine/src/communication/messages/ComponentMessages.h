#pragma once

#include "GPUComponentMessage.h"
#include <blazingdb/transport/Message.h>
#include <blazingdb/transport/common/macros.hpp>

namespace ral {
namespace communication {
namespace messages {

struct SampleToNodeMasterMessage : GPUComponentMessage {
	SampleToNodeMasterMessage(const std::string & message_token,
		const uint32_t & context_token,
		std::shared_ptr<Node> & sender_node,
		std::vector<gdf_column_cpp> & samples,
		int total_row_size)
		: GPUComponentMessage(message_token, context_token, sender_node, samples, total_row_size) {}

	DefineClassName(SampleToNodeMasterMessage);

	std::size_t getTotalRowSize() const { return this->metadata().total_row_size; }

	static std::shared_ptr<GPUMessage> MakeFrom(const Message::MetaData & message_metadata,
		const Address::MetaData & address_metadata,
		const std::vector<ColumnTransport> & columns_offsets,
		const std::vector<const char *> & raw_buffers) {
		return GPUComponentMessage::MakeFrom(message_metadata, address_metadata, columns_offsets, raw_buffers);
	}
};

struct ColumnDataMessage : GPUComponentMessage {
	ColumnDataMessage(const std::string & message_token,
		const uint32_t & context_token,
		std::shared_ptr<Node> & sender_node,
		std::vector<gdf_column_cpp> & samples)
		: GPUComponentMessage(message_token, context_token, sender_node, samples) {}

	DefineClassName(ColumnDataMessage);

	std::vector<gdf_column_cpp> getColumns() { return this->samples; }

	static std::shared_ptr<GPUMessage> MakeFrom(const Message::MetaData & message_metadata,
		const Address::MetaData & address_metadata,
		const std::vector<ColumnTransport> & columns_offsets,
		const std::vector<const char *> & raw_buffers) {
		return GPUComponentMessage::MakeFrom(message_metadata, address_metadata, columns_offsets, raw_buffers);
	}
};

struct PartitionPivotsMessage : GPUComponentMessage {
	PartitionPivotsMessage(const std::string & message_token,
		const uint32_t & context_token,
		std::shared_ptr<Node> & sender_node,
		std::vector<gdf_column_cpp> & samples)
		: GPUComponentMessage(message_token, context_token, sender_node, samples) {}

	DefineClassName(PartitionPivotsMessage);

	std::vector<gdf_column_cpp> getColumns() { return this->samples; }

	static std::shared_ptr<GPUMessage> MakeFrom(const Message::MetaData & message_metadata,
		const Address::MetaData & address_metadata,
		const std::vector<ColumnTransport> & columns_offsets,
		const std::vector<const char *> & raw_buffers) {
		return GPUComponentMessage::MakeFrom(message_metadata, address_metadata, columns_offsets, raw_buffers);
	}
};

}  // namespace messages
}  // namespace communication
}  // namespace ral
