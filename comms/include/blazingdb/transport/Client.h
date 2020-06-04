#pragma once

#include <exception>
#include <memory>

#include "blazingdb/transport/Address.h"
#include "blazingdb/transport/Message.h"
#include "blazingdb/transport/Node.h"
#include "blazingdb/transport/Status.h"

namespace blazingdb {
namespace transport {

class Client {
public:
  class SendError;

  virtual Status Send(GPUMessage& message) = 0;

  virtual bool notifyLastMessageEvent(const Message::MetaData &message_metadata) = 0;

  virtual void Close() = 0;
};

class ClientTCP : public Client {
public:
  virtual Status Send(GPUMessage& message) = 0;

  virtual void Close() = 0;

  static std::shared_ptr<Client> Make(const std::string& ip, int16_t port);
};

}  // namespace transport
}  // namespace blazingdb
