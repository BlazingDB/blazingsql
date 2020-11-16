#include "Node.h"

#include <iostream>

namespace blazingdb {
namespace transport {

Node::Node(){}

Node::Node(const std::string& id, bool isAvailable)
    : id_{id}, isAvailable_{isAvailable} {}

bool Node::operator==(const Node &rhs) const {
  return id_ == rhs.id_;
}

bool Node::operator!=(const Node &rhs) const {
  return !(*this == rhs);
}

std::string Node::id() const { return id_; }

bool Node::isAvailable() const { return isAvailable_; }

void Node::setAvailable(bool available) { isAvailable_ = available; }

void Node::print() const {
  std::string isAvailable = isAvailable_ ? "true" : "false";
  std::cout << "NODE: isAvailable_: " << isAvailable << "|" 
            << "|id: " << id_;
}

}  // namespace transport
}  // namespace blazingdb
