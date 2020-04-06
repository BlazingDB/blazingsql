#include "communication/factory/MessageFactory.h"


namespace ral {
namespace communication {
namespace messages {
namespace experimental {

std::shared_ptr<Message> Factory::createSampleToNodeMaster(const std::string & message_token,
														   const ContextToken & context_token,
														   Node  & sender_node,
														   std::uint64_t total_row_size,
														   const ral::frame::BlazingTableView & samples) {
	return std::make_shared<SampleToNodeMasterMessage>(
		message_token, context_token, sender_node, samples, total_row_size);
}

std::shared_ptr<Message> Factory::createColumnDataMessage(const std::string & message_token,
														  const ContextToken & context_token,
														  Node & sender_node,
														  const ral::frame::BlazingTableView & columns) {
	return std::make_shared<ColumnDataMessage>(message_token, context_token, sender_node, columns);
}

std::shared_ptr<Message> Factory::createPartitionPivotsMessage(const std::string & message_token,
															   const ContextToken & context_token,
															   Node  & sender_node,
															   const ral::frame::BlazingTableView & columns) {
	return std::make_shared<PartitionPivotsMessage>(message_token, context_token, sender_node, columns);
}

}  // namespace experimental
}  // namespace messages
}  // namespace communication
}  // namespace ral
