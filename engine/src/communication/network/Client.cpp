#include "communication/network/Client.h"
// #include <blazingdb/manager/Manager.h>
#include <blazingdb/transport/Client.h>
#include <blazingdb/transport/api.h>

namespace ral {
namespace communication {
namespace network {

// concurrent::send
Status Client::send(const Node & node, GPUMessage & message) {
	const auto & metadata = node.address().metadata();
	auto ral_client = blazingdb::transport::experimental::ClientTCP::Make(metadata.ip, metadata.comunication_port);
	return ral_client->Send(message);
}

bool Client::notifyLastMessageEvent(const Node & node, const Message::MetaData &message_metadata) {
	const auto & metadata = node.address().metadata();
	auto ral_client = blazingdb::transport::experimental::ClientTCP::Make(metadata.ip, metadata.comunication_port);
	return ral_client->notifyLastMessageEvent(message_metadata);
}

void Client::closeConnections() {
	// for (auto& c : ral_clients) {
	//   ral_clients[c.first]->Close();
	// }
}


Status Client::sendNodeData(std::string ip, int16_t port, Message & message) {
	// TOOO: Remove old code: 
	// NOTE No more Manager classes
	assert(false);
	// auto client = blazingdb::manager::Manager::MakeClient(ip, port);
	// return client->Send(message);
	return Status{};
}

}  // namespace network
}  // namespace communication
}  // namespace ral
