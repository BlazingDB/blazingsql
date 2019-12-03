#pragma once

#include <memory>
#include <mutex>
#include <vector>
#include "blazingdb/transport/Node.h"

namespace blazingdb {
namespace manager {

using Node = blazingdb::transport::Node;

/// \brief Represents a set of Node instances
class Cluster {
public:
  explicit Cluster() = default;

  void addNode(const Node &node);

  size_t getTotalNodes() const;

  std::vector<std::shared_ptr<Node>> getAvailableNodes(int clusterSize);

private:
  std::vector<std::shared_ptr<Node>> nodes_;

  std::mutex condition_mutex;
};

}  // namespace manager
}  // namespace blazingdb
