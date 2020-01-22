#pragma once

#include "communication/messages/ComponentMessages.h"
#include <blazingdb/transport/Message.h>
#include <blazingdb/transport/Node.h>
#include <vector>

namespace ral {
namespace communication {
namespace messages {
namespace experimental {

using Node = blazingdb::transport::experimental::Node;
using Message = blazingdb::transport::experimental::GPUMessage;
using ContextToken = uint32_t;

struct Factory {
	
	static std::shared_ptr<Message> createSampleToNodeMaster(const std::string & message_token,
															 const ContextToken & context_token,
															 Node & sender_node,
															 std::uint64_t total_row_size,
															 const ral::frame::BlazingTableView & samples);

	static std::shared_ptr<Message> createColumnDataMessage(const std::string & message_token,
															const ContextToken & context_token,
															Node & sender_node,
															const ral::frame::BlazingTableView & columns);

	static std::shared_ptr<Message> createPartitionPivotsMessage(const std::string & message_token,
																 const ContextToken & context_token,
																 Node & sender_node,
																 const ral::frame::BlazingTableView & columns);
};

}  // namespace experimental
}  // namespace messages
}  // namespace communication
}  // namespace ral
