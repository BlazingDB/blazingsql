#pragma once

#include <condition_variable>
#include <mutex>
#include <string>
#include <vector>
#include <set>
#include <map>

#include "blazingdb/transport/Message.h"

namespace blazingdb {
namespace transport {
namespace experimental {

class MessageQueue {
public:
  MessageQueue();

  ~MessageQueue() = default;

  MessageQueue(MessageQueue&&) = delete;

  MessageQueue(const MessageQueue&) = delete;

  MessageQueue& operator=(MessageQueue&&) = delete;

  MessageQueue& operator=(const MessageQueue&) = delete;

public:
  std::shared_ptr<ReceivedMessage> getMessage(const std::string& messageToken);

  void putMessage(std::shared_ptr<ReceivedMessage>& message);

  
private:
  std::shared_ptr<ReceivedMessage> getMessageQueue(const std::string& messageToken);

  void putMessageQueue(std::shared_ptr<ReceivedMessage>& message);

private:
  std::mutex mutex_;
  std::vector<std::shared_ptr<ReceivedMessage>> message_queue_;
  std::condition_variable condition_variable_;
};
}  // namespace experimental
}  // namespace transport
}  // namespace blazingdb
