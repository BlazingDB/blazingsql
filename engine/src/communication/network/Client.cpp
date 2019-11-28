#include <blazingdb/manager/Manager.h>
#include "communication/network/Client.h"
#include <blazingdb/transport/Client.h>
#include <blazingdb/transport/api.h>
#include "config/GPUManager.cuh"

namespace ral {
namespace communication {
namespace network {

// static std::map<std::pair<std::string, int16_t>, std::shared_ptr<blazingdb::transport::Client> > ral_clients;

// concurrent::send
blazingdb::transport::Status Client::send(const Node &node,
                                          GPUMessage &message) {
  const auto& metadata = node.address()->metadata();
  auto ral_client = blazingdb::transport::ClientTCP::Make(
                          metadata.ip,
                          metadata.comunication_port);
  ral_client->SetDevice( ral::config::GPUManager::getInstance().getDeviceId() );
  return ral_client->Send(message);
}

void Client::closeConnections() {
  // for (auto& c : ral_clients) {
  //   ral_clients[c.first]->Close();
  // }  
}


blazingdb::transport::Status Client::sendNodeData(std::string ip, int16_t port,
                                                  Message &message) {
  auto client = blazingdb::manager::Manager::MakeClient(ip, port);
  return client->Send(message);
}

} // namespace network
} // namespace communication
} // namespace ral
