#include "communication/network/Server.h"
#include "communication/messages/ComponentMessages.h"
#include "communication/messages/GPUComponentMessage.h"
#include "config/GPUManager.cuh"

namespace ral {
namespace communication {
namespace network {
namespace experimental {

unsigned short Server::port_ = 8000;
std::map<int, Server *> servers_;

// [static]
void Server::start(unsigned short port) {
	port_ = port;
	if(servers_.find(port_) != servers_.end()) {
		throw std::runtime_error("[server-ral] with the same port");
	}
	servers_[port_] = new Server();
}

// [static]
void Server::close() { getInstance().comm_server->Close(); }

// [static]
Server & Server::getInstance() { return *servers_[port_]; }

Server::Server() {
	comm_server = CommServer::TCP(port_);
	setEndPoints();
	comm_server->Run();
}

Server::~Server() {}

void Server::registerContext(const ContextToken context_token) { comm_server->registerContext(context_token); }

void Server::deregisterContext(const ContextToken context_token) { comm_server->deregisterContext(context_token); }

std::shared_ptr<GPUReceivedMessage> Server::getMessage(
	const ContextToken & token_value, const MessageTokenType & messageToken) {
	return comm_server->getMessage(token_value, messageToken);
}

void Server::setEndPoints() {
	// message SampleToNodeMasterMessage
	{
		const std::string endpoint = ral::communication::messages::experimental::SampleToNodeMasterMessage::MessageID();
		comm_server->registerEndPoint(endpoint);
		comm_server->registerMessageForEndPoint(ral::communication::messages::experimental::SampleToNodeMasterMessage::MakeFrom, endpoint);

	// 	  using MakeCallback = std::function<std::shared_ptr<GPUMessage>(
    //   const Message::MetaData &, const Address::MetaData &,
    //   const std::vector<ColumnTransport> &, const std::vector<rmm::device_buffer> &)>;
	}

	// message ColumnDataMessage
	{
		const std::string endpoint = ral::communication::messages::experimental::ColumnDataMessage::MessageID();
		comm_server->registerEndPoint(endpoint);
		comm_server->registerMessageForEndPoint(ral::communication::messages::experimental::ColumnDataMessage::MakeFrom, endpoint);
	}

	// message PartitionPivotsMessage
	{
		const std::string endpoint = ral::communication::messages::experimental::PartitionPivotsMessage::MessageID();
		comm_server->registerEndPoint(endpoint);
		comm_server->registerMessageForEndPoint(ral::communication::messages::experimental::PartitionPivotsMessage::MakeFrom, endpoint);
	}
}
}  // namespace experimental
}  // namespace network
}  // namespace communication
}  // namespace ral
