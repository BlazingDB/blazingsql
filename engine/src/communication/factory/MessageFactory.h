#pragma once

#include "communication/messages/ComponentMessages.h"
#include <blazingdb/transport/Message.h>
#include <blazingdb/transport/Node.h>
#include <vector>

namespace ral {
namespace communication {
namespace messages {

using Node = blazingdb::transport::Node;
using Message = blazingdb::transport::GPUMessage;
using ContextToken = uint32_t;

struct Factory {
	static std::shared_ptr<Message> createSampleToNodeMaster(const std::string & message_token,
		const ContextToken & context_token,
		std::shared_ptr<Node> & sender_node,
		std::uint64_t total_row_size,
		std::vector<gdf_column_cpp> samples);

	static std::shared_ptr<Message> createColumnDataMessage(const std::string & message_token,
		const ContextToken & context_token,
		std::shared_ptr<Node> & sender_node,
		std::vector<gdf_column_cpp> columns);

	static std::shared_ptr<Message> createPartitionPivotsMessage(const std::string & message_token,
		const ContextToken & context_token,
		std::shared_ptr<Node> & sender_node,
		std::vector<gdf_column_cpp> columns);

	static std::shared_ptr<Message> createSampleToNodeMaster(const std::string & message_token,
															 const ContextToken & context_token,
															 std::shared_ptr<Node> & sender_node,
															 std::uint64_t total_row_size,
															 std::unique_ptr<ral::frame::BlazingTableView>&& samples);

	static std::shared_ptr<Message> createColumnDataMessage(const std::string & message_token,
															const ContextToken & context_token,
															std::shared_ptr<Node> & sender_node,
															std::unique_ptr<ral::frame::BlazingTableView>&& columns);

	static std::shared_ptr<Message> createPartitionPivotsMessage(const std::string & message_token,
																 const ContextToken & context_token,
																 std::shared_ptr<Node> & sender_node,
																 std::unique_ptr<ral::frame::BlazingTableView>&& columns);
};

}  // namespace messages
}  // namespace communication
}  // namespace ral
