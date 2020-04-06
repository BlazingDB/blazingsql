#include "blazingdb/transport/Client.h"
#include <cuda_runtime_api.h>
#include <map>
#include <numeric>
#include "blazingdb/network/TCPSocket.h"
#include "blazingdb/transport/ColumnTransport.h"
#include "blazingdb/transport/Status.h"
#include "blazingdb/transport/io/reader_writer.h"

namespace blazingdb {
namespace transport {

namespace experimental {

class Client::SendError : public std::exception {
public:
  SendError(const std::string& original, const std::string& endpoint,
            const std::string& data, const std::size_t bufferSize)
      : endpoint_{endpoint},
        data_{data},
        bufferSize_{bufferSize},
        message_{original + " transport::Client: Bad endpoint \"" + endpoint_ +
                 "\" with buffer size = " + std::to_string(bufferSize_) +
                 " and data = " + data_} {}

  const char* what() const noexcept override { return message_.c_str(); }

private:
  const std::string endpoint_;
  const std::string data_;
  const std::size_t bufferSize_;
  const std::string message_;
};

template <typename MetadataType>
void write_metadata(void* file_descriptor, const MetadataType& metadata) {
  blazingdb::transport::io::writeToSocket(file_descriptor, (char*)&metadata,
                                          sizeof(MetadataType));
}

class ConcreteClientTCP : public ClientTCP {
public:
  ConcreteClientTCP(const std::string& ip, int16_t port)
      : client_socket{ip, port} {}
  void Close() override { client_socket.close(); }

  void SetDevice(int gpuId) override { this->gpuId = gpuId; }

  Status Send(GPUMessage& message) override {
    void* fd = client_socket.fd();
    auto &node = message.getSenderNode();
    auto message_metadata = message.metadata();

    // send message metadata
    write_metadata(fd, message_metadata);
    // send address metadata
    write_metadata(fd, node.address().metadata_);

    // send message content (gpu buffers)
    std::vector<int> buffer_sizes;
    std::vector<const char *> buffers;
    std::vector<ColumnTransport> column_offsets;
    std::vector<std::unique_ptr<rmm::device_buffer>> temp_scope_holder;
    std::tie(buffer_sizes, buffers, column_offsets, temp_scope_holder) = message.GetRawColumns();

    write_metadata(fd, (int32_t)column_offsets.size());
    blazingdb::transport::io::writeToSocket(
        fd, (char*)column_offsets.data(),
        sizeof(ColumnTransport) * column_offsets.size());

    write_metadata(fd, (int32_t)buffer_sizes.size());
    blazingdb::transport::io::writeToSocket(fd, (char*)buffer_sizes.data(),
                                            sizeof(int) * buffer_sizes.size());

    blazingdb::transport::experimental::io::writeBuffersFromGPUTCP(column_offsets, buffer_sizes, buffers, fd, gpuId);
    blazingdb::transport::io::writeToSocket(fd, "OK", 2, false);

    zmq::socket_t* socket_ptr = (zmq::socket_t*)fd;

    int data_past_topic{0};
    auto data_past_topic_size{sizeof(data_past_topic)};
    socket_ptr->getsockopt(ZMQ_RCVMORE, &data_past_topic,
                           &data_past_topic_size);
    // receive the ok
    zmq::message_t local_message;
    auto success = socket_ptr->recv(local_message);
    if (success.value() == false || local_message.size() == 0) {
      std::cerr << "Client:   throw zmq::error_t()" << std::endl;
      throw zmq::error_t();
    }

    std::string end_message(static_cast<char*>(local_message.data()),
                            local_message.size());
    assert(end_message == "END");
    return Status{true};
  }

protected:
  blazingdb::network::TCPClientSocket client_socket;
  int gpuId{0};
};

std::shared_ptr<Client> ClientTCP::Make(const std::string& ip, int16_t port) {
  return std::shared_ptr<Client>(new ConcreteClientTCP(ip, port));
}

}  // namespace experimental
}  // namespace transport
}  // namespace blazingdb
