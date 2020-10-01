#include "communication/network/Server.h"
#include "communication/messages/ComponentMessages.h"

namespace ral {
namespace communication {
namespace network {

unsigned short Server::port_ = 8000;
bool Server::use_batch_processing_ = false;
std::map<int, Server *> servers_;

// [static]
void Server::start(unsigned short port, bool use_batch_processing) {
	port_ = port;
	use_batch_processing_ = use_batch_processing;
	if(servers_.find(port_) != servers_.end()) {
		throw std::runtime_error("[server-ral] with the same port");
	}
	servers_[port_] = new Server();
}




Server::Server() {
	if (use_batch_processing_ == true) {
		comm_server = CommServer::BatchProcessing(port_);
	} else {
		comm_server = CommServer::TCP(port_);
	};
	setEndPoints();
	comm_server->Run();
}

Server::~Server() {}

void Server::registerContext(const ContextToken context_token) { comm_server->registerContext(context_token); }

void Server::deregisterContext(const ContextToken context_token) { comm_server->deregisterContext(context_token); }

std::shared_ptr<ReceivedMessage> Server::getMessage(
	const ContextToken & token_value, const MessageTokenType & messageToken) {
	auto message = comm_server->getMessage(token_value, messageToken);
	
	messages::ReceivedHostMessage * host_msg_ptr = nullptr;
	if (host_msg_ptr = dynamic_cast<messages::ReceivedHostMessage *>(message.get())) {
		std::string messageToken = message->getMessageTokenValue();
		uint32_t contextToken = message->getContextTokenValue();
	 	auto sender_node = message->getSenderNode();
		std::unique_ptr<ral::frame::BlazingTable> samples = host_msg_ptr->getBlazingTable();
		int64_t total_row_size = message->metadata().total_row_size;
		int32_t partition_id = message->metadata().partition_id;
		return std::make_shared<messages::ReceivedDeviceMessage>(messageToken, contextToken, sender_node, std::move(samples), total_row_size, partition_id);
	}
	
	return message;
}

std::shared_ptr<ReceivedMessage> Server::getHostMessage(const ContextToken & token_value, const MessageTokenType & messageToken){
	return comm_server->getMessage(token_value, messageToken);
}

void Server::setEndPoints() {
	// device messages
	{
		// message SampleToNodeMasterMessage
		{
			const std::string endpoint = ral::communication::messages::SampleToNodeMasterMessage::MessageID();
			comm_server->registerEndPoint(endpoint);
			comm_server->registerDeviceDeserializerForEndPoint(ral::communication::messages::SampleToNodeMasterMessage::MakeFrom, endpoint);
		}

		// message ColumnDataMessage
		{
			const std::string endpoint = ral::communication::messages::ColumnDataMessage::MessageID();
			comm_server->registerEndPoint(endpoint);
			comm_server->registerDeviceDeserializerForEndPoint(ral::communication::messages::ColumnDataMessage::MakeFrom, endpoint);
		}

		// message ColumnDataPartitionMessage
		{
			const std::string endpoint = ral::communication::messages::ColumnDataPartitionMessage::MessageID();
			comm_server->registerEndPoint(endpoint);
			comm_server->registerDeviceDeserializerForEndPoint(ral::communication::messages::ColumnDataPartitionMessage::MakeFrom, endpoint);
		}

		// message PartitionPivotsMessage
		{
			const std::string endpoint = ral::communication::messages::PartitionPivotsMessage::MessageID();
			comm_server->registerEndPoint(endpoint);
			comm_server->registerDeviceDeserializerForEndPoint(ral::communication::messages::PartitionPivotsMessage::MakeFrom, endpoint);
		}
	}
	///// Host Messages
	{
		{
			const std::string endpoint = ral::communication::messages::SampleToNodeMasterMessage::MessageID();
			comm_server->registerEndPoint(endpoint);
			comm_server->registerHostDeserializerForEndPoint(ral::communication::messages::GPUComponentMessage::MakeFromHost, endpoint);
		}

		// message ColumnDataMessage
		{
			const std::string endpoint = ral::communication::messages::ColumnDataMessage::MessageID();
			comm_server->registerEndPoint(endpoint);
			comm_server->registerHostDeserializerForEndPoint(ral::communication::messages::GPUComponentMessage::MakeFromHost, endpoint);
		}

		// message ColumnDataMessage
		{
			const std::string endpoint = ral::communication::messages::ColumnDataPartitionMessage::MessageID();
			comm_server->registerEndPoint(endpoint);
			comm_server->registerHostDeserializerForEndPoint(ral::communication::messages::GPUComponentMessage::MakeFromHost, endpoint);
		}

		// message PartitionPivotsMessage
		{
			const std::string endpoint = ral::communication::messages::PartitionPivotsMessage::MessageID();
			comm_server->registerEndPoint(endpoint);
			comm_server->registerHostDeserializerForEndPoint(ral::communication::messages::GPUComponentMessage::MakeFromHost, endpoint);
		}
	}
}

}  // namespace network
}  // namespace communication
}  // namespace ral
