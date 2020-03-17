
#include "blazingdb/transport/Server.h"
#include "blazingdb/network/TCPSocket.h"
#include "blazingdb/transport/io/reader_writer.h"

#include <cuda_runtime_api.h>
#include <condition_variable>
#include <deque>
#include <map>
#include <mutex>
#include <numeric>
#include <shared_mutex>
#include <string>
#include <thread>
#include <tuple>
#include <vector>
#include "blazingdb/network/TCPSocket.h"
#include "blazingdb/transport/MessageQueue.h"
#include <rmm/device_buffer.hpp>

namespace blazingdb {
namespace transport {

namespace experimental {

void Server::registerEndPoint(const std::string &end_point) {
  end_points_.emplace_back(end_point);
}

void Server::registerMessageForEndPoint(MakeCallback deserializer,
                                        const std::string &end_point) {
  deserializer_[end_point] = deserializer;
}

void Server::registerContext(const uint32_t context_token) {
  std::unique_lock<std::shared_timed_mutex> lock(context_messages_mutex_);
  context_messages_map_.emplace(std::piecewise_construct,
                                std::forward_as_tuple(context_token),
                                std::tuple<>());
}

void Server::deregisterContext(const uint32_t context_token) {
  std::unique_lock<std::shared_timed_mutex> lock(context_messages_mutex_);
  auto it = context_messages_map_.find(context_token);
  if (it != context_messages_map_.end()) {
    context_messages_map_.erase(it);
  }
}

std::shared_ptr<GPUReceivedMessage> Server::getMessage(
    const uint32_t context_token, const std::string &messageToken) {
  MessageQueue &message_queue = context_messages_map_.at(context_token);
  return  message_queue.getMessage(messageToken);
}

void Server::putMessage(const uint32_t context_token,
                        std::shared_ptr<GPUReceivedMessage> &message) {
  std::shared_lock<std::shared_timed_mutex> lock(context_messages_mutex_);
  if (context_messages_map_.find(context_token) ==
      context_messages_map_.end()) {
    // putMessage could be called before registerContext so we force the
    // registration of the context token.
    lock.unlock();
    registerContext(context_token);
    lock.lock();
  }

  MessageQueue &message_queue = context_messages_map_.at(context_token);
  message_queue.putMessage(message);
}

Server::MakeCallback Server::getDeserializationFunction(
    const std::string &endpoint) {
  const auto &iterator = deserializer_.find(endpoint);
  if (iterator == deserializer_.end()) {
    throw std::runtime_error("deserializer not found: " + endpoint);
  }
  return iterator->second;
}

template <typename MetadataType>
MetadataType read_metadata(void *fd) {
  MetadataType metadata;
  blazingdb::transport::io::readFromSocket(fd, (char *)&metadata,
                                           sizeof(MetadataType));
  return metadata;
}

namespace {

class ServerTCP : public Server {
public:
  ServerTCP(unsigned short port) : server_socket{port} {}

  void SetDevice(int gpuId) override { this->gpuId = gpuId; }

  void Run() override;

  void Close() override;

  ~ServerTCP() { thread.join(); }

private:
  /**
   * Simple-TPC-Server
   */
  blazingdb::network::TCPServerSocket server_socket;

  std::thread thread;

  int gpuId{0};
};

void connectionHandler(ServerTCP *server, void *socket, int gpuId) {
  try {
    // use io reader to read the message
    // READ these two values, end point should be fixed width string
    // so you read sizeof(EndPointSizeInStruct) + sizeof(size_t)
    // read that from the buffer to get these values

    // begin of message
    Message::MetaData message_metadata =
        read_metadata<Message::MetaData>(socket);
    Address::MetaData address_metadata =
        read_metadata<Address::MetaData>(socket);

    // read columns (gpu buffers)
    auto column_offset_size = read_metadata<int32_t>(socket);
    std::vector<ColumnTransport> column_offsets(column_offset_size);
    blazingdb::transport::io::readFromSocket(
        socket, (char *)column_offsets.data(),
        column_offset_size * sizeof(ColumnTransport));

    auto buffer_sizes_size = read_metadata<int32_t>(socket);
    std::vector<int> buffer_sizes(buffer_sizes_size);
    blazingdb::transport::io::readFromSocket(
        socket, (char *)buffer_sizes.data(), buffer_sizes_size * sizeof(int));

    std::vector<rmm::device_buffer> raw_columns;
    blazingdb::transport::experimental::io::readBuffersIntoGPUTCP(buffer_sizes, socket, gpuId, raw_columns);
    zmq::socket_t *socket_ptr = (zmq::socket_t *)socket;

    int data_past_topic{0};
    auto data_past_topic_size{sizeof(data_past_topic)};
    socket_ptr->getsockopt(ZMQ_RCVMORE, &data_past_topic,
                           &data_past_topic_size);
    if (data_past_topic == 0 || data_past_topic_size == 0) {
      std::cerr << "Server: No data inside message." << std::endl;
      return;
    }
    // receive the ok
    zmq::message_t local_message;
    auto success = socket_ptr->recv(local_message);
    if (success.value() == false || local_message.size() == 0) {
      throw zmq::error_t();
    }

    std::string ok_message(static_cast<char *>(local_message.data()),
                           local_message.size());
    assert(ok_message == "OK");
    blazingdb::transport::io::writeToSocket(socket, "END", 3, false);
    // end of message

    std::string messageToken = message_metadata.messageToken;
    auto deserialize_function = server->getDeserializationFunction(
        messageToken.substr(0, messageToken.find('_')));
    std::shared_ptr<GPUReceivedMessage> message = deserialize_function(message_metadata, address_metadata, column_offsets, raw_columns);
    assert(message != nullptr);
    server->putMessage(message->metadata().contextToken, message);

    // TODO: write success
  } catch (const zmq::error_t &e) {
    // TODO: write failure
  }
  catch (const std::runtime_error &exception) {
    std::cerr << "[ERROR] " << exception.what() << std::endl;
    // TODO: write failure
  }
}

void ServerTCP::Run() {
  thread = std::thread([this]() {
    server_socket.run([this](void *fd) {
      connectionHandler(this, fd, this->gpuId);
    });
  });
  std::this_thread::yield();
}
void ServerTCP::Close() { this->server_socket.close(); }

}  // namespace

std::unique_ptr<Server> Server::TCP(unsigned short port) {
  return std::unique_ptr<Server>(new ServerTCP(port));
}

}  // namespace experimental
}  // namespace transport
}  // namespace blazingdb
