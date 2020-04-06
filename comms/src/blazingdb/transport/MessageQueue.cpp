#include "blazingdb/transport/MessageQueue.h"
#include <algorithm>
#include <iostream>

namespace blazingdb {
namespace transport {
namespace experimental {

std::shared_ptr<GPUReceivedMessage> MessageQueue::getMessage(
    const std::string &messageToken) {
  std::unique_lock<std::mutex> lock(mutex_);
  condition_variable_.wait(lock, [&, this] {
    return std::any_of(this->message_queue_.cbegin(),
                       this->message_queue_.cend(), [&](const auto &e) {
                         return e->getMessageTokenValue() == messageToken;
                       });
  });

  return getMessageQueue(messageToken);
}

void MessageQueue::putMessage(std::shared_ptr<GPUReceivedMessage> &message) {
  std::unique_lock<std::mutex> lock(mutex_);
  putMessageQueue(message);
  lock.unlock();
  condition_variable_.notify_all(); // Note: Very important to notify all threads
}

std::shared_ptr<GPUReceivedMessage> MessageQueue::getMessageQueue(
    const std::string &messageToken) {
  auto it = std::partition(message_queue_.begin(), message_queue_.end(),
                           [&messageToken](const auto &e) {
                             return e->getMessageTokenValue() != messageToken;
                           });
  assert(it != message_queue_.end());

  std::shared_ptr<GPUReceivedMessage> message = *it;
  message_queue_.erase(it, it + 1);

  return message;
}

void MessageQueue::putMessageQueue(std::shared_ptr<GPUReceivedMessage> &message) {
  message_queue_.push_back(message);
}
}  // namespace experimental
}  // namespace transport
}  // namespace blazingdb
