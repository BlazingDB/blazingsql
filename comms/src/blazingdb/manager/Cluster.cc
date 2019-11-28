#include "blazingdb/manager/Cluster.h"

#include <algorithm>
#include <iostream>

namespace blazingdb {
namespace manager {

void Cluster::addNode(const Node &node) {
  std::unique_lock<std::mutex> lock(condition_mutex);

  if (std::find_if(nodes_.cbegin(), nodes_.cend(),
                   [&](auto &n) { return *n == node; }) != nodes_.end()) {
    // TODO: workaround until implement a proper discovery and heartbeat for
    // workers If the node crashed and its trying to register again with the
    // same ip, ignore it because its already on the cluster
    return;
  }

  // TODO, update cluster interface
  nodes_.push_back(std::make_shared<Node>(node));
}

size_t Cluster::getTotalNodes() const { return nodes_.size(); }

std::vector<std::shared_ptr<Node>> Cluster::getAvailableNodes(int clusterSize) {
  std::unique_lock<std::mutex> lock(condition_mutex);

  std::vector<std::shared_ptr<Node>> availableNodes;
  auto copyIter = std::back_inserter(availableNodes);
  auto iterFirst = nodes_.begin();
  auto iterLast = nodes_.end();
  while (iterFirst != iterLast && clusterSize > 0) {
    if ((*iterFirst)->isAvailable())
      *copyIter++ = *iterFirst;
    ++iterFirst;
    --clusterSize;
  }

  return availableNodes;
}

} // namespace manager
} // namespace blazingdb
