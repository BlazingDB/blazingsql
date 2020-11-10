#pragma once

#include <vector>
#include <map>
#include <mutex>
#include "blazingdb/transport/Node.h"

namespace blazingdb {
namespace manager {

using Node = blazingdb::transport::Node;

/// \brief This is the main component of the transport library
///
/// Manage and group the Node 's (task nodes) to be used by an specific query.
class Context {
public:
  explicit Context(const uint32_t token,
                   const std::vector<Node>& taskNodes,
                   const Node& masterNode,
                   const std::string& logicalPlan,
                   const std::map<std::string, std::string>& config_options);

      // TODO Cristhian Gonzalez no copies allowed
  std::shared_ptr<Context> clone();

  int getTotalNodes() const;

  std::vector<Node> getAllNodes() const;

  std::vector<Node> getAllOtherNodes(int selfNodeIndex) const;

  /// RAL instances that will run the query
  std::vector<Node> getWorkerNodes() const;

  Node getNode(int node_index) const;
  Node getNode(const std::string & id) const;

  /// A single unique RAL instance that helps to the messages transmition and
  /// processesing between worker RAL's e.g.: see SampleToNodeMasterMessage
  const Node& getMasterNode() const;

  /// @deprecated: not used anymore
  std::string getLogicalPlan() const;

  uint32_t getContextToken() const;
  std::string getContextCommunicationToken() const;

  void incrementQueryStep();
  void incrementQuerySubstep();

  uint32_t getQueryStep() const { return query_step; };
  uint32_t getQuerySubstep() const { return query_substep; };

  int getNodeIndex(const Node& node) const;
  bool isMasterNode(const Node& node) const;

  void setKernelId(uint32_t kernel_id) {
    this->kernel_id_ = kernel_id;
  }
  uint32_t getKernelId() const {
    return this->kernel_id_;
  }
  std::map<std::string, std::string> getConfigOptions() const {
    return config_options_;
  }

private:
  const uint32_t token_;
  uint32_t query_step;
  uint32_t query_substep;
  const std::vector<Node> taskNodes_;
  const Node masterNode_;
  const std::string logicalPlan_;
  uint32_t kernel_id_;
  std::mutex increment_step_mutex;
  std::map<std::string, std::string> config_options_;
};

}  // namespace manager
}  // namespace blazingdb
