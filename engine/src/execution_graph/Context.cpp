#include "Context.h"
#include <algorithm>

namespace blazingdb {
namespace manager {

Context::Context(const uint32_t token,
                 const std::vector<Node> &taskNodes,
                 const Node &masterNode,
                 const std::string &logicalPlan,
                 const std::map<std::string, std::string>& config_options,
                 const std::string current_timestamp)
    : token_{token},
      query_step{0},
      query_substep{0},
      taskNodes_{taskNodes},
      masterNode_{masterNode},
      logicalPlan_{logicalPlan},
      kernel_id_{0},
      config_options_{config_options},
      current_timestamp_{current_timestamp} {}

std::shared_ptr<Context> Context::clone() {
  auto ptr = std::make_shared<Context>(this->token_, this->taskNodes_, this->masterNode_, this->logicalPlan_, this->config_options_, this->current_timestamp_);
  ptr->query_step = this->query_step;
  ptr->query_substep = this->query_substep;
  ptr->kernel_id_ = this->kernel_id_;
  ptr->current_timestamp_ = this->current_timestamp_;
  return ptr;
}

int Context::getTotalNodes() const { return taskNodes_.size(); }

std::vector<Node> Context::getAllNodes() const {
  return taskNodes_;
}

std::vector<Node> Context::getAllOtherNodes(
    int selfNodeIndex) const {
  std::vector<Node> siblings(taskNodes_.size() - 1);
  size_t count = 0;
  for (size_t i = 0; i < taskNodes_.size(); i++) {
    if (i != static_cast<size_t>(selfNodeIndex)) {
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

Node Context::getNode(const std::string & id) const {
  auto it = std::find_if(taskNodes_.begin(), taskNodes_.end(), [id](auto& n) { return n.id() == id; });
  if(it == taskNodes_.end()){
    throw std::runtime_error("Invalid node id");
  }
  return *it;
}

const Node &Context::getMasterNode() const { return masterNode_; }

std::string Context::getLogicalPlan() const { return logicalPlan_; }

std::string Context::getCurrentTimestamp() const { return current_timestamp_; }

uint32_t Context::getContextToken() const { return token_; }

std::string Context::getContextCommunicationToken() const {
  return std::to_string(kernel_id_) + "_"  + std::to_string(query_substep) ;
}

void Context::incrementQueryStep() {
  std::unique_lock<std::mutex> lock(increment_step_mutex);

  query_step++;
  query_substep++;
}

void Context::incrementQuerySubstep() {
  std::unique_lock<std::mutex> lock(increment_step_mutex);
  query_substep++;
}

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

}  // namespace manager
}  // namespace blazingdb
