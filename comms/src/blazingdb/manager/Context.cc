#include "blazingdb/manager/Context.h"
#include <algorithm>
#include <climits>

namespace blazingdb {
namespace manager {
namespace experimental { 

Context::Context(const uint32_t token,
                 const std::vector<Node> &taskNodes,
                 const Node &masterNode,
                 const std::string &logicalPlan)
    : token_{token},
      taskNodes_{taskNodes},
      masterNode_{masterNode},
      logicalPlan_{logicalPlan},
      query_step{0},
      query_substep{0} {}

int Context::getTotalNodes() const { return taskNodes_.size(); }

std::vector<Node> Context::getAllNodes() const {
  return taskNodes_;
}

std::vector<Node> Context::getAllOtherNodes(
    int selfNodeIndex) const {
  std::vector<Node> siblings(taskNodes_.size() - 1);
  size_t count = 0;
  for (size_t i = 0; i < taskNodes_.size(); i++) {
    if (i != selfNodeIndex) {
      siblings[count] = taskNodes_[i];
      count++;
    }
  }
  return siblings;
}

std::vector<Node> Context::getWorkerNodes() const {
  std::vector<Node> siblings;
  std::copy_if(taskNodes_.cbegin(), taskNodes_.cend(),
               std::back_inserter(siblings),
               [this](const Node &n) {
                 return !(n == this->masterNode_);
               });
  return siblings;
}

Node Context::getNode(int node_index) const{
  return taskNodes_[node_index];
}

const Node &Context::getMasterNode() const { return masterNode_; }

std::string Context::getLogicalPlan() const { return logicalPlan_; }

uint32_t Context::getContextToken() const { return token_; }

uint32_t Context::getContextCommunicationToken() const { return query_substep; }

void Context::incrementQueryStep() {
  query_step++;
  query_substep++;
}

void Context::incrementQuerySubstep() { query_substep++; }

int Context::getNodeIndex(const Node &node) const {
  auto it =
      std::find_if(taskNodes_.cbegin(), taskNodes_.cend(),
                   [&](const Node &n) { return n == node; });

  if (it == taskNodes_.cend()) {
    return -1;
  }

  return std::distance(taskNodes_.cbegin(), it);
}

bool Context::isMasterNode(const Node &node) const {
  return masterNode_ == node;
}

}  // namespace experimental
}  // namespace manager
}  // namespace blazingdb
