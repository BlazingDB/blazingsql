#pragma once

#include <cstring>
#include <iostream>
#include <memory>
#include <string>
#include "blazingdb/transport/common/macros.hpp"

namespace blazingdb {
namespace transport {
namespace experimental {

/// \brief Represents the address (ip:port)
///
/// This is used in the constructor of each Node
/// The Orchestrator use this also through the Manager
/// also the other Node 's through the Server

class Address {
public:
  enum Type { TCP_TYPE, IPC_TYPE, UCX_IPC_TYPE, UCX_GDR_TYPE, NONE };

  static constexpr int ADDRSTRLEN = 16;

  struct MetaData {
    int32_t type{NONE};
    char ip[ADDRSTRLEN]{};
    int16_t comunication_port{};
    int16_t protocol_port{};

    bool operator==(const MetaData &rhs) const {
      return type == rhs.type and std::string{ip} == std::string{rhs.ip} and
             comunication_port == rhs.comunication_port and
             protocol_port == rhs.protocol_port;
    }
  } metadata_;
  Address();
  Address(const Address& address);

  const MetaData &metadata() const { return metadata_; }

protected:
  explicit Address(Type type, const std::string &ip,
                   const std::int16_t communication_port,
                   const std::int16_t protocol_port);

public:
  static Address TCP(const std::string &ip,
                                      const std::int16_t communication_port,
                                      const std::int16_t protocol_port);  
};

}  // namespace experimental
}  // namespace transport
}  // namespace blazingdb
