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
  void notifyLastMessage(const std::string& messageToken);

  void setNumberOfBatches(const std::string& messageToken, size_t n_batches);

  size_t getNumberOfBatches(const std::string& messageToken);

  std::shared_ptr<GPUReceivedMessage> getMessage(const std::string& messageToken);

  void putMessage(std::shared_ptr<GPUReceivedMessage>& message);

  
private:
  std::shared_ptr<GPUReceivedMessage> getMessageQueue(const std::string& messageToken);

  void putMessageQueue(std::shared_ptr<GPUReceivedMessage>& message);

private:
  std::mutex mutex_;
  std::set<std::string>  last_message_flags_;
  std::map<std::string, size_t>  n_batches_map_;
  std::vector<std::shared_ptr<GPUReceivedMessage>> message_queue_;
  std::condition_variable condition_variable_;
  std::condition_variable condition_variable_n_batches_;
};
}  // namespace experimental
}  // namespace transport
}  // namespace blazingdb
