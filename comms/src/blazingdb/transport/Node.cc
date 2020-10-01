#include "blazingdb/transport/Node.h"

#include <iostream>

namespace blazingdb {
namespace transport {

Node::Node(){}

Node::Node(const Address &address, const std::string& id, bool isAvailable)
    : address_{address}, id_{id}, isAvailable_{isAvailable} {}

bool Node::operator==(const Node &rhs) const {
  return address_.metadata_ == rhs.address_.metadata_ && id_ == rhs.id_;
}

std::string Node::id() const { return id_; }

const Address& Node::address() const noexcept { return address_; }

bool Node::isAvailable() const { return isAvailable_; }

void Node::setAvailable(bool available) { isAvailable_ = available; }

void Node::print() const {
  std::string isAvailable = isAvailable_ ? "true" : "false";
  const auto& metadata = this->address_.metadata_;
  std::cout << "NODE: isAvailable_: " << isAvailable << "|" << metadata.ip
            << "|id: " << id_
            << "|comunication_port: " << metadata.comunication_port
            << "|protocol_port:" << metadata.protocol_port << "\n";
}

}  // namespace transport
}  // namespace blazingdb
