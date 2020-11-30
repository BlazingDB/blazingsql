#pragma once

#include <memory>
#include <string>

namespace blazingdb {
namespace transport {

/// \brief A Node is the representation of a RAL component used in the transport
/// process.
class Node {
public:
  // TODO define clear constructors
  Node();
  Node( const std::string& id, bool isAvailable = true);

  bool operator==(const Node& rhs) const;
  bool operator!=(const Node& rhs) const;


  std::string id() const;

  /// Note: Not implemented yet
  bool isAvailable() const;

  /// Note: Not implemented yet
  void setAvailable(bool available);

  void print() const;


protected:

  std::string id_;
  bool isAvailable_{false};
};

}  // namespace transport
}  // namespace blazingdb
