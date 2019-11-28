#include "config/GPUManager.cuh"
#include "communication/network/Server.h"
#include "communication/messages/ComponentMessages.h"
#include "communication/messages/GPUComponentMessage.h"

namespace ral {
namespace communication {
namespace network {

    unsigned short Server::port_ = 8000;
    std::map<int, Server*> servers_;

    // [static]
    void Server::start(unsigned short port) {
        port_ = port;
        if (servers_.find(port_) != servers_.end()) {
          throw std::runtime_error("[server-ral] with the same port");
        }
        servers_[port_] = new Server();
    }

    // [static]
    void Server::close() {
      getInstance().comm_server->Close();
    }

    // [static]
    Server& Server::getInstance() {
        return *servers_[port_];
    }

    Server::Server() {
        comm_server = CommServer::TCP(port_);
        setEndPoints();
        ral::config::GPUManager::getInstance().setDevice();
        comm_server->SetDevice( ral::config::GPUManager::getInstance().getDeviceId() );
        comm_server->Run();
    }

    Server::~Server() {
    }

    void Server::registerContext(const ContextToken context_token) {
        comm_server->registerContext(context_token);
    }

    void Server::deregisterContext(const ContextToken context_token) {
        comm_server->deregisterContext(context_token);
    }

    std::shared_ptr<GPUMessage> Server::getMessage(const ContextToken& token_value, const MessageTokenType& messageToken) {
        return comm_server->getMessage(token_value, messageToken);
    }

    void Server::setEndPoints() {

        // message SampleToNodeMasterMessage
        {
            const std::string endpoint = messages::SampleToNodeMasterMessage::MessageID();
            comm_server->registerEndPoint(endpoint );
            comm_server->registerMessageForEndPoint(ral::communication::messages::SampleToNodeMasterMessage::MakeFrom, endpoint);
        }

        // message ColumnDataMessage
        {
          const std::string endpoint = messages::ColumnDataMessage::MessageID();
          comm_server->registerEndPoint(endpoint );
          comm_server->registerMessageForEndPoint(ral::communication::messages::ColumnDataMessage::MakeFrom, endpoint);
        }

        // message PartitionPivotsMessage
        {
          const std::string endpoint = messages::PartitionPivotsMessage::MessageID();
          comm_server->registerEndPoint(endpoint );
          comm_server->registerMessageForEndPoint(ral::communication::messages::PartitionPivotsMessage::MakeFrom, endpoint);
        }
    }

} // namespace network
} // namespace communication
} // namespace ral
