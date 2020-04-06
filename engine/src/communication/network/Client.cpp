#include "communication/network/Client.h"
#include "config/GPUManager.cuh"
// #include <blazingdb/manager/Manager.h>
#include <blazingdb/transport/Client.h>
#include <blazingdb/transport/api.h>

namespace ral {
namespace communication {
namespace network {
namespace experimental{
// static std::map<std::pair<std::string, int16_t>, std::shared_ptr<blazingdb::transport::Client> > ral_clients;

// concurrent::send
Status Client::send(const Node & node, GPUMessage & message) {
	const auto & metadata = node.address().metadata();
	auto ral_client = blazingdb::transport::experimental::ClientTCP::Make(metadata.ip, metadata.comunication_port);
	return ral_client->Send(message);
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
}  // namespace experimental
}  // namespace network
}  // namespace communication
}  // namespace ral
