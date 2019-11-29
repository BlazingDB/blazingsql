#include "blazingdb/transport/ColumnTransport.h"
#include "blazingdb/transport/Message.h"
#include "blazingdb/manager/NodeDataMessage.h"

namespace blazingdb {
namespace manager {

std::shared_ptr<Message> NodeDataMessage::Make(std::shared_ptr<Node> &node) {
  return std::make_shared<NodeDataMessage>(node);
}

}  // namespace manager
}  // namespace blazingdb
