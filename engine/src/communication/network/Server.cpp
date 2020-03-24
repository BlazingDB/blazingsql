#include "communication/network/Server.h"
#include "communication/messages/ComponentMessages.h"
#include "communication/messages/GPUComponentMessage.h"
#include "config/GPUManager.cuh"

namespace ral {
namespace communication {
namespace network {
namespace experimental {

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

// [static]
void Server::close() { getInstance().comm_server->Close(); }

// [static]
Server & Server::getInstance() { return *servers_[port_]; }

Server::Server() {
	if (use_batch_processing_ == true) {
		comm_server = CommServer::BatchProcessing(port_);
	} else {
		comm_server = CommServer::TCP(port_);
	};
	setEndPoints();
	if (use_batch_processing_ == false) {
		comm_server->Run();
	}
}
Server::~Server() {}

void Server::registerContext(const ContextToken context_token) { comm_server->registerContext(context_token); }

void Server::deregisterContext(const ContextToken context_token) { comm_server->deregisterContext(context_token); }

std::shared_ptr<GPUReceivedMessage> Server::getMessage(
	const ContextToken & token_value, const MessageTokenType & messageToken) {
	return comm_server->getMessage(token_value, messageToken);
}
void Server::handle(HostCallback callback){
	//	Ensure just one call
	static int counter = {0};
	assert(counter == 0);
	assert(use_batch_processing_);
	comm_server->Run(callback);
	counter++;
}

void Server::setEndPoints() {
	// message SampleToNodeMasterMessage
	{
		const std::string endpoint = ral::communication::messages::experimental::SampleToNodeMasterMessage::MessageID();
		comm_server->registerEndPoint(endpoint);
		comm_server->registerDeviceDeserializerForEndPoint(ral::communication::messages::experimental::SampleToNodeMasterMessage::MakeFrom, endpoint);
	}

	// message ColumnDataMessage
	{
		const std::string endpoint = ral::communication::messages::experimental::ColumnDataMessage::MessageID();
		comm_server->registerEndPoint(endpoint);
		comm_server->registerDeviceDeserializerForEndPoint(ral::communication::messages::experimental::ColumnDataMessage::MakeFrom, endpoint);
	}

	// message PartitionPivotsMessage
	{
		const std::string endpoint = ral::communication::messages::experimental::PartitionPivotsMessage::MessageID();
		comm_server->registerEndPoint(endpoint);
		comm_server->registerDeviceDeserializerForEndPoint(ral::communication::messages::experimental::PartitionPivotsMessage::MakeFrom, endpoint);
	}

	// Host Messages
	{
		const std::string endpoint = ral::communication::messages::experimental::SampleToNodeMasterMessage::MessageID();
		comm_server->registerEndPoint(endpoint);
		comm_server->registerHostDeserializerForEndPoint(ral::communication::messages::experimental::GPUComponentMessage::MakeFromHost, endpoint);
	}

	// message ColumnDataMessage
	{
		const std::string endpoint = ral::communication::messages::experimental::ColumnDataMessage::MessageID();
		comm_server->registerEndPoint(endpoint);
		comm_server->registerHostDeserializerForEndPoint(ral::communication::messages::experimental::GPUComponentMessage::MakeFromHost, endpoint);
	}

	// message PartitionPivotsMessage
	{
		const std::string endpoint = ral::communication::messages::experimental::PartitionPivotsMessage::MessageID();
		comm_server->registerEndPoint(endpoint);
		comm_server->registerHostDeserializerForEndPoint(ral::communication::messages::experimental::GPUComponentMessage::MakeFromHost, endpoint);
	}
}
}  // namespace experimental
}  // namespace network
}  // namespace communication
}  // namespace ral
