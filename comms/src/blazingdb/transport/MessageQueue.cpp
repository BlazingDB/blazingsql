#include "blazingdb/transport/MessageQueue.h"
#include <algorithm>
#include <iostream>

namespace blazingdb {
namespace transport {
namespace experimental {

MessageQueue::MessageQueue() 
  : last_message_flags_{}
{

}

std::shared_ptr<ReceivedMessage> MessageQueue::getMessage(
    const std::string &messageToken) {
  std::unique_lock<std::mutex> lock(mutex_);

  condition_variable_.wait(lock, [&, this] {
    bool last_msg = this->last_message_flags_.find(messageToken) != this->last_message_flags_.end();
    return last_msg or std::any_of(this->message_queue_.cbegin(),
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

void MessageQueue::notifyLastMessage(const std::string& messageToken) {
  std::unique_lock<std::mutex> lock(mutex_);
  this->last_message_flags_.insert(messageToken);
  bool last_msg = this->last_message_flags_.find(messageToken) != this->last_message_flags_.end();

  std::cout << "notify last>>> " <<  messageToken << "|" << last_msg << std::endl;
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
  auto it = std::partition(message_queue_.begin(), message_queue_.end(),
                           [&messageToken](const auto &e) {
                             return e->getMessageTokenValue() != messageToken;
                           });
  bool last_msg = this->last_message_flags_.find(messageToken) != this->last_message_flags_.end();
  if (last_msg) {
    std::cerr << "TODO: Return Last sentinel Message\n";
    return nullptr;
  }
  assert(it != message_queue_.end());

  std::shared_ptr<ReceivedMessage> message = *it;
  message_queue_.erase(it, it + 1);

  return message;
}

void MessageQueue::putMessageQueue(std::shared_ptr<ReceivedMessage> &message) {
  message_queue_.push_back(message);
}
}  // namespace experimental
}  // namespace transport
}  // namespace blazingdb
