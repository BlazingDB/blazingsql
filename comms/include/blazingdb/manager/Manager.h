#pragma once

#include <memory>
#include <vector>

#include "blazingdb/transport/Client.h"
#include "blazingdb/manager/Cluster.h"
#include "blazingdb/manager/Context.h"

namespace blazingdb {
namespace manager {

class ManagerClient {
public:

  virtual transport::Status Send(transport::Message& message) = 0;

};

/// \brief This is a server used only by the Orchestrator
///
/// This class will register the Node instances (i.e. RAL instances)
class Manager {
public:
  Manager() = default;

  virtual void Run() = 0;

  virtual void Close() = 0;

  virtual const Cluster& getCluster() const = 0;

  virtual Context* generateContext(std::string logicalPlan, int clusterSize, uint32_t context_token) = 0;

public:

  static std::unique_ptr<Manager> MakeServer(int communicationTcpPort);

  static std::unique_ptr<ManagerClient> MakeClient(const std::string &ip, std::uint16_t port);
};

}  // namespace manager
}  // namespace blazingdb
