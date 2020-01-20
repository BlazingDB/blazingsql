#include <blazingdb/transport/api.h>

#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <chrono>
#include <memory>
#include <thread>

namespace blazingdb {
namespace transport {
// Alias

class MockFlag {
public:
  MOCK_METHOD0(Flag, void());
};
constexpr uint32_t context_token = 1234;

class ComponentMessage : public GPUMessage {
public:
  ComponentMessage(uint32_t contextToken, std::shared_ptr<Node> &sender_node,
                   std::uint64_t total_row_size)
      : GPUMessage(ComponentMessage::MessageID(), contextToken, sender_node) {
    this->metadata().total_row_size =
        total_row_size;  // TODO: create custom extra_info metadata??
  }

  virtual raw_buffer GetRawColumns() {
    std::vector<int> bufferSizes;
    std::vector<const char *> buffers;
    std::vector<ColumnTransport> column_offset;

    return std::make_tuple(bufferSizes, buffers, column_offset);
  }

  static std::shared_ptr<GPUMessage> MakeFrom(
      const Message::MetaData &message_metadata,
      const Address::MetaData &address_metadata,
      const std::vector<ColumnTransport> &columns_offsets,
      const std::vector<char *> &raw_buffers) {
    auto node = std::make_shared<Node>(
        Address::TCP(address_metadata.ip, address_metadata.comunication_port,
                     address_metadata.protocol_port));
    return std::make_shared<ComponentMessage>(
        message_metadata.contextToken, node, message_metadata.total_row_size);
  }

  DefineClassName(ComponentMessage);
};

TEST(IntegrationServerClientTest, SendMessageToServerFromClient) {
  // Create server
  std::unique_ptr<Server> server = Server::TCP(8000);

  // Configure server
  auto endpoint = ComponentMessage::MessageID();
  server->registerEndPoint(endpoint);
  server->registerContext(context_token);

  server->registerMessageForEndPoint(ComponentMessage::MakeFrom, endpoint);

  // Run server in a thread
  server->Run();
  std::this_thread::sleep_for(std::chrono::seconds(1));

  // Create message
  auto node = std::make_shared<Node>(Address::TCP("localhost", 8000, 9999));
  ComponentMessage message{context_token, node, 0};

  auto client = blazingdb::transport::ClientTCP::Make("localhost", 8000);
  try {
    auto status = client->Send(message);
    EXPECT_TRUE(status.IsOk());
  } catch (std::exception &e) {
    FAIL() << e.what();
  }

  // Get a frame
  MockFlag mockFlag;
  EXPECT_CALL(mockFlag, Flag()).Times(1);

  std::thread serverGetMessageThread([&server, &mockFlag, &endpoint]() {
    std::shared_ptr<Message> message =
        server->getMessage(context_token, endpoint);
    auto mock_message = std::dynamic_pointer_cast<ComponentMessage>(message);
    mockFlag.Flag();
  });
  // Clean context
  serverGetMessageThread.join();
}
}  // namespace transport
}  // namespace blazingdb
