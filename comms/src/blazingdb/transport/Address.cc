#ifndef BLAZINGDB_COMMUNICATION_ADDRESS_INTERNAL_H_
#define BLAZINGDB_COMMUNICATION_ADDRESS_INTERNAL_H_

#include "blazingdb/transport/Address.h"
#include <cassert>

namespace blazingdb {
namespace transport {
namespace experimental {

Address::Address(){}
 
Address::Address(const Address& address){
  this->metadata_ = address.metadata_;
  memcpy(this->metadata_.ip, address.metadata_.ip, ADDRSTRLEN);
}

Address::Address(Type type, const std::string &ip,
                 const std::int16_t communication_port,
                 const std::int16_t protocol_port)
    : metadata_{} {
  this->metadata_.type = type;
  assert(ip.size() < ADDRSTRLEN - 1);
  memcpy(this->metadata_.ip, ip.c_str(), ip.size());
  this->metadata_.comunication_port = communication_port;
  this->metadata_.protocol_port = protocol_port;
}

class TCPAddress : public Address {
public:
  TCPAddress(const std::string &ip, const std::int16_t communication_port,
             const std::int16_t protocol_port)
      : Address(Type::TCP_TYPE, ip, communication_port, protocol_port) {}

  virtual ~TCPAddress() = default;
};
Address Address::TCP(const std::string &ip,
                                      const std::int16_t communication_port,
                                      const std::int16_t protocol_port) {
  return TCPAddress(ip, communication_port, protocol_port);
}

}  // namespace experimental
}  // namespace transport
}  // namespace blazingdb

#endif
