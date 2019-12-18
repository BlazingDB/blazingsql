#include "blazingdb/manager/Manager.h"
#include "blazingdb/manager/Cluster.h"
#include "blazingdb/manager/Context.h"

#include <algorithm>
#include <functional>
#include "blazingdb/network/TCPSocket.h"
#include "blazingdb/transport/io/fd_reader_writer.h"

namespace blazingdb {
namespace manager {

using namespace blazingdb::transport;

static std::string s_recv(zmq::socket_t &socket) {
  zmq::message_t message;
  socket.recv(&message);

  return std::string(static_cast<char *>(message.data()), message.size());
}

class ManagerTCP : public Manager {
public:
  ManagerTCP(int tcpPort) {
    this->socket = zmq::socket_t(context, ZMQ_REP);
    auto connection = "tcp://*:" + std::to_string(tcpPort);
    socket.bind(connection);
  }

  void Run() override final {
    thread = std::thread([this]() {
      while (socket.connected()) {
        try {
          zmq::message_t request;
          socket.recv(&request);

          Address::MetaData address_metadata;
          memcpy((char *)&address_metadata, request.data(),
                 sizeof(Address::MetaData));
          auto address = Address::TCP(address_metadata.ip,
                                      address_metadata.comunication_port,
                                      address_metadata.protocol_port);
          auto node = Node(address);
          this->cluster_.addNode(node);

          socket.send(zmq::message_t("OK", 2));
        } catch (zmq::error_t &e) {
          // std::cout << "W: interrupt received, proceeding…" << std::endl;
          break;
        }
      }
    });
  }
  void Close() {
    this->socket.close();
    this->context.close();
  }

  const Cluster &getCluster() const { return cluster_; };

  Context *generateContext(std::string logicalPlan, int clusterSize,
                           uint32_t context_token) final {
    auto taskNodes = cluster_.getAvailableNodes(clusterSize);
    if (taskNodes.size() > 0) {
      runningTasks_.push_back(std::make_unique<Context>(
          context_token, taskNodes, taskNodes.at(0), logicalPlan));
    } else {
      std::cerr << "CERR: Context with null taskNode\n";
      runningTasks_.push_back(std::make_unique<Context>(
          context_token, taskNodes, nullptr, logicalPlan));
    }
    return runningTasks_.back().get();
  }

  ~ManagerTCP() { thread.join(); }

private:
  std::thread thread;
  zmq::context_t context{1};
  zmq::socket_t socket;

  Cluster cluster_;
  std::vector<std::unique_ptr<Context>> runningTasks_;
};

class ClientTCPForManager : public ManagerClient {
public:
  ClientTCPForManager(const std::string &ip, const std::uint16_t port) {
    socket = zmq::socket_t(context, ZMQ_REQ);
    auto connection = "tcp://" + ip + ":" + std::to_string(port);
    socket.connect(connection);
  }

  Status Send(Message &message) override {
    auto node = message.getSenderNode();
    auto address_metadata = node->address()->metadata();
    zmq::message_t request((void *)&address_metadata,
                           sizeof(Address::MetaData));
    socket.send(request);

    zmq::message_t reply;
    socket.recv(&reply);
    auto ok_message =
        std::string(static_cast<char *>(reply.data()), reply.size());
    assert(ok_message == "OK");
    return Status{true};
  }

protected:
  zmq::context_t context{1};
  zmq::socket_t socket;
};

std::unique_ptr<Manager> Manager::MakeServer(int communicationTcpPort) {
  return std::unique_ptr<Manager>{new ManagerTCP(communicationTcpPort)};
}

std::unique_ptr<ManagerClient> Manager::MakeClient(const std::string &ip,
                                                   uint16_t port) {
  return std::make_unique<ClientTCPForManager>(ip, port);
}

}  // namespace manager
}  // namespace blazingdb
