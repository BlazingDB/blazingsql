#pragma once

#include <memory>

#include "blazingdb/transport/Address.h"

namespace blazingdb {
namespace transport {
namespace experimental {

/// \brief A Node is the representation of a RAL component used in the transport
/// process.
class Node {
public:
  // TODO define clear constructors
  Node();
  Node(const Address & address, bool isAvailable = true);

  bool operator==(const Node& rhs) const;
  bool operator!=(const Node& rhs) const;

  const Address& address() const noexcept;

  /// Note: Not implemented yet
  bool isAvailable() const;

  /// Note: Not implemented yet
  void setAvailable(bool available);

  void print() const;
 

protected:
  Address address_;
  bool isAvailable_{false};
};

}  // namespace experimental
}  // namespace transport
}  // namespace blazingdb
