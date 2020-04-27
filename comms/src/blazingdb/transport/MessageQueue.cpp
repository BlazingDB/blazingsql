#include "blazingdb/transport/MessageQueue.h"
#include <algorithm>
#include <iostream>

namespace blazingdb {
namespace transport {
namespace experimental {

MessageQueue::MessageQueue()
{

}

std::shared_ptr<ReceivedMessage> MessageQueue::getMessage(
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

void MessageQueue::putMessage(std::shared_ptr<ReceivedMessage> &message) {
  std::unique_lock<std::mutex> lock(mutex_);
  putMessageQueue(message);
  lock.unlock();
  condition_variable_.notify_all(); // Note: Very important to notify all threads
}

void MessageQueue::setNumberOfBatches(const std::string& messageToken, size_t n_batches) {
  std::unique_lock<std::mutex> lock(mutex_);
  this->n_batches_map_[messageToken] = n_batches;
  condition_variable_n_batches_.notify_all(); // Note: Very important to notify all threads
}

size_t MessageQueue::getNumberOfBatches(const std::string& messageToken) {
  std::unique_lock<std::mutex> lock(mutex_);

  condition_variable_n_batches_.wait(lock, [&, this] {
    auto iter = this->n_batches_map_.find(messageToken);
    return iter != this->n_batches_map_.end();
  });
  return this->n_batches_map_[messageToken];
}

std::shared_ptr<ReceivedMessage> MessageQueue::getMessageQueue(
    const std::string &messageToken) {
  auto it = std::find_if(message_queue_.begin(), message_queue_.end(),
                           [&messageToken](const auto &e) {
                             return e->getMessageTokenValue() == messageToken;
                           });
  assert(it != message_queue_.end());

  std::shared_ptr<ReceivedMessage> message = *it;
  message_queue_.erase(it, it + 1);

  if (message->is_sentinel()) {
    return nullptr;
  }

  return message;
}

void MessageQueue::putMessageQueue(std::shared_ptr<ReceivedMessage> &message) {
  message_queue_.push_back(message);
}
}  // namespace experimental
}  // namespace transport
}  // namespace blazingdb
