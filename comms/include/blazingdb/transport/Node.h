#pragma once

#include <memory>
#include <string>

#include "blazingdb/transport/Address.h"

namespace blazingdb {
namespace transport {

/// \brief A Node is the representation of a RAL component used in the transport
/// process.
class Node {
public:
  // TODO define clear constructors
  Node();
  Node(const Address & address, const std::string& id, bool isAvailable = true);

  bool operator==(const Node& rhs) const;
  bool operator!=(const Node& rhs) const;

  const Address& address() const noexcept;

  std::string id() const;

  /// Note: Not implemented yet
  bool isAvailable() const;

  /// Note: Not implemented yet
  void setAvailable(bool available);

  void print() const;


protected:
  Address address_;
  std::string id_;
  bool isAvailable_{false};
};

}  // namespace transport
}  // namespace blazingdb
