#pragma once
#include <cassert>
#include <memory>
#include <vector>
#include "blazingdb/transport/Message.h"
#include "blazingdb/transport/Node.h"
#include "blazingdb/transport/common/macros.hpp"

namespace blazingdb {
namespace manager {

using Node = blazingdb::transport::Node;
using Message = blazingdb::transport::Message;

class NodeDataMessage : public Message {
public:
  NodeDataMessage(std::shared_ptr<Node> &node, uint32_t context_token = 0)
      : Message(NodeDataMessage::MessageID(), context_token, node) {}

  static std::shared_ptr<Message> Make(std::shared_ptr<Node> &node);

  DefineClassName(NodeDataMessage);
};

}  // namespace manager
}  // namespace blazingdb
