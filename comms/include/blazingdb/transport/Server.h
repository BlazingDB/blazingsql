#pragma once

#include <functional>
#include <map>
#include <memory>
#include <shared_mutex>
#include <string>
#include "MessageQueue.h"
#include "blazingdb/transport/Message.h"
#include <rmm/device_buffer.hpp>

namespace blazingdb {
namespace transport {
namespace experimental {

class Server {
public:
  /**
   * Alias of the message class used in the implementation of the server.
   */
  using MakeCallback = std::function<std::shared_ptr<GPUReceivedMessage>(
      const Message::MetaData &, const Address::MetaData &,
      const std::vector<ColumnTransport> &, const std::vector<rmm::device_buffer> &)>;

public:
  virtual ~Server() = default;

  /**
   * It is used to map an endpoint with the static deserialize function (Make)
   * implemented in the different messages.
   *
   * @param end_point     name of the endpoint.
   * @param deserializer  function used to deserialize the message.
   */
  virtual void registerMessageForEndPoint(MakeCallback deserializer,
                                          const std::string &end_point);

public:
  /**
   * It is used to create an endpoint and choose the HTTP method for that
   * endpoint. The endpoint create has the structure: '/message/' + endpoint
   *
   * @param end_point  name of the endpoint.
   * @param method     select HTTP Method.
   */
  virtual void registerEndPoint(const std::string &end_point);

public:
  /**
   * It is used to create a new message queue.
   * The new message queue will be related to the ContextToken.
   * It uses a 'unique lock' with a 'shared_mutex' in order to ensure unique
   * access to the whole structure.
   * @param context_token  ContextToken class identifier.
   */
  virtual void registerContext(const uint32_t context_token);

  /**
   * It is used to destroy a message queue related to the ContextToken.
   * It uses a 'unique lock' with a shared_mutex in order to ensure unique
   * access to the whole structure.
   *
   * @param context_token  ContextToken class identifier.
   */
  virtual void deregisterContext(const uint32_t context_token);

public:
  /**
   * It starts the server.
   * It is required to configure the server before it is started.
   * Use the function 'registerContext', 'registerEndPoint' and
   * 'registerDeserializer' for configuration.
   *
   * @param port  the port for the server. Default value '8000'.
   */
  virtual void Run() = 0;

  virtual void Close() = 0;

  virtual void SetDevice(int) = 0;

public:
  /**
   * It retrieves the message that it is stored in the message queue.
   * In case that the message queue is empty and the function is called, the
   * function will wait until the server receives a new message with the same
   * ContextToken. Each message queue works independently, which means that the
   * wait condition has no relationship between message queues. It uses a
   * 'shared lock' with a 'shared mutex' for that purpose.
   *
   * @param context_token  identifier for the message queue using ContextToken.
   * @return               a shared pointer of a base message class.
   */
  virtual std::shared_ptr<GPUReceivedMessage> getMessage(
      const uint32_t context_token, const std::string &messageToken);

  /**
   * It stores the message in the message queue and it uses the ContextToken to
   * select the queue. Each message queue works independently. Whether multiple
   * threads want to access at the same time to different message queue, then
   * all the threads put the message in the corresponding queue without wait or
   * exclusion.
   *
   * @param context_token  identifier for the message queue using ContextToken.
   * @param message        message that will be stored in the corresponding
   * queue.
   */
  virtual void putMessage(const uint32_t context_token,
                          std::shared_ptr<GPUReceivedMessage> &message);

  //
  Server::MakeCallback getDeserializationFunction(const std::string &endpoint);

protected:
  /**
   * Defined in 'shared_mutex' header file.
   * It allows access of multiple threads (shared) or only one thread
   * (exclusive). It will be used to protect access to context_messages_map_.
   */
  std::shared_timed_mutex context_messages_mutex_;

  /**
   * It associate the context value with a message queue.
   */
  std::map<uint32_t, MessageQueue> context_messages_map_;

  /**
   * It associates the endpoint to a HTTP Method.
   */
  std::vector<std::string> end_points_;

  /**
   * It associate the endpoint with a unique deserialize message function.
   */
  std::map<std::string, MakeCallback> deserializer_;

public:
  /**
   * Static function that creates a TCP server.
   *
   * @return  unique pointer of the TCP server.
   */
  static std::unique_ptr<Server> TCP(unsigned short port);
};
}  // namespace experimental
}  // namespace transport
}  // namespace blazingdb
