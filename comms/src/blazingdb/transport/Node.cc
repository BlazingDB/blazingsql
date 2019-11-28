#include "blazingdb/transport/Node.h"

#include <iostream>
#include <string>

namespace blazingdb {
namespace transport {

Node::Node(const std::shared_ptr<Address> &address, bool isAvailable)
    : address_{address}, isAvailable_{isAvailable} {}

bool Node::operator==(const Node &rhs) const {
  return address_->metadata() == rhs.address_->metadata();
}

std::shared_ptr<Address> Node::address() const noexcept { return address_; }

bool Node::isAvailable() const { return isAvailable_; }

void Node::setAvailable(bool available) { isAvailable_ = available; }

void Node::print() const {
  std::string isAvailable = isAvailable_ ? "true" : "false";
  auto metadata = this->address()->metadata();
  std::cout << "NODE: isAvailable_: " << isAvailable << "|" << metadata.ip
            << "|comunication_port: " << metadata.comunication_port<< "|protocol_port:"
            <<  metadata.protocol_port << "\n";
}

std::shared_ptr<blazingdb::transport::Node> Node::Make(
    const std::shared_ptr<Address> &address) {
  return std::make_shared<blazingdb::transport::Node>(address);
}

}  // namespace transport
}  // namespace blazingdb
