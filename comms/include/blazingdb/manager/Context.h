#pragma once

#include <vector>

#include <vector>
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
                   const std::vector<std::shared_ptr<Node>>& taskNodes,
                   const std::shared_ptr<Node>& masterNode,
                   const std::string& logicalPlan);

  int getTotalNodes() const;

  std::vector<std::shared_ptr<Node>> getAllNodes() const;

  std::vector<std::shared_ptr<Node>> getAllOtherNodes(int selfNodeIndex) const;

  /// RAL instances that will run the query
  std::vector<std::shared_ptr<Node>> getWorkerNodes() const;

  /// A single unique RAL instance that helps to the messages transmition and
  /// processesing between worker RAL's e.g.: see SampleToNodeMasterMessage
  const Node& getMasterNode() const;

  /// @deprecated: not used anymore
  std::string getLogicalPlan() const;

  uint32_t getContextToken() const;
  uint32_t getContextCommunicationToken() const;

  void incrementQueryStep();
  void incrementQuerySubstep();

  uint32_t getQueryStep() const { return query_step; };
  uint32_t getQuerySubstep() const { return query_substep; };

  int getNodeIndex(const Node& node) const;
  bool isMasterNode(const Node& node) const;

private:
  const uint32_t token_;
  uint32_t query_step;
  uint32_t query_substep;
  const std::vector<std::shared_ptr<Node>> taskNodes_;
  const std::shared_ptr<Node> masterNode_;
  const std::string logicalPlan_;
};

}  // namespace manager
}  // namespace blazingdb


namespace blazingdb {
namespace manager {
namespace experimental {

using Node = blazingdb::transport::experimental::Node;

/// \brief This is the main component of the transport library
///
/// Manage and group the Node 's (task nodes) to be used by an specific query.
class Context {
public:
  explicit Context(const uint32_t token,
                   const std::vector<Node>& taskNodes,
                   const Node& masterNode,
                   const std::string& logicalPlan);

      // TODO Cristhian Gonzalez no copies allowed

  int getTotalNodes() const;

  std::vector<Node> getAllNodes() const;

  std::vector<Node> getAllOtherNodes(int selfNodeIndex) const;

  /// RAL instances that will run the query
  std::vector<Node> getWorkerNodes() const;

  Node getNode(int node_index) const;

  /// A single unique RAL instance that helps to the messages transmition and
  /// processesing between worker RAL's e.g.: see SampleToNodeMasterMessage
  const Node& getMasterNode() const;

  /// @deprecated: not used anymore
  std::string getLogicalPlan() const;

  uint32_t getContextToken() const;
  uint32_t getContextCommunicationToken() const;

  void incrementQueryStep();
  void incrementQuerySubstep();

  uint32_t getQueryStep() const { return query_step; };
  uint32_t getQuerySubstep() const { return query_substep; };

  int getNodeIndex(const Node& node) const;
  bool isMasterNode(const Node& node) const;

private:
  const uint32_t token_;
  uint32_t query_step;
  uint32_t query_substep;
  const std::vector<Node> taskNodes_;
  const Node masterNode_;
  const std::string logicalPlan_;
};

}  // namespace experimental
}  // namespace manager
}  // namespace blazingdb
