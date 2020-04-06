#include "blazingdb/transport/Node.h"

#include <iostream>
#include <string>

namespace blazingdb {
namespace transport {
namespace experimental {

Node::Node(){}

Node::Node(const Address &address, bool isAvailable)
    : address_{address}, isAvailable_{isAvailable} {}

bool Node::operator==(const Node &rhs) const {
  return address_.metadata_ == rhs.address_.metadata_;
}

const Address& Node::address() const noexcept { return address_; }

bool Node::isAvailable() const { return isAvailable_; }

void Node::setAvailable(bool available) { isAvailable_ = available; }

void Node::print() const {
  std::string isAvailable = isAvailable_ ? "true" : "false";
  const auto& metadata = this->address_.metadata_;
  std::cout << "NODE: isAvailable_: " << isAvailable << "|" << metadata.ip
            << "|comunication_port: " << metadata.comunication_port
            << "|protocol_port:" << metadata.protocol_port << "\n";
}

}  // namespace experimental
}  // namespace transport
}  // namespace blazingdb
