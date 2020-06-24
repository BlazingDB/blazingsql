#include <blazingdb/transport/api.h>

#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <chrono>
#include <memory>
#include <thread>
#include <rmm/device_buffer.hpp>

namespace blazingdb {
namespace transport {

class MockFlag {
public:
  MOCK_METHOD0(Flag, void());
};
constexpr uint32_t context_token = 1234;

class ComponentMessage;
struct ReceivedDeviceMessage : public ReceivedMessage {
	ReceivedDeviceMessage(uint32_t contextToken, const Node &sender_node,
		std::uint64_t total_row_size)
	: ReceivedMessage(ReceivedDeviceMessage::MessageID(), contextToken, sender_node){
		this->metadata().total_row_size = total_row_size;
	}
	DefineClassName(ComponentMessage);
};

class ComponentMessage : public GPUMessage {
public:
  ComponentMessage(uint32_t contextToken, const Node &sender_node,
                   std::uint64_t total_row_size)
      : GPUMessage(ComponentMessage::MessageID(), contextToken, sender_node) {
    this->metadata().total_row_size =
        total_row_size;  // TODO: create custom extra_info metadata??
  }

  virtual raw_buffer GetRawColumns() {
    std::vector<std::size_t> bufferSizes;
    std::vector<const char *> buffers;
    std::vector<ColumnTransport> column_offset;
    std::vector<std::unique_ptr<rmm::device_buffer>> temp_scope_holder;

    return std::make_tuple(bufferSizes, buffers, column_offset, std::move(temp_scope_holder));
  }

  static std::shared_ptr<ReceivedMessage> MakeFrom(
      const Message::MetaData &message_metadata,
      const Address::MetaData &address_metadata,
      const std::vector<ColumnTransport> &columns_offsets,
       const std::vector<rmm::device_buffer> &raw_buffers) {
    Node node(
        Address::TCP(address_metadata.ip, address_metadata.comunication_port,
                     address_metadata.protocol_port), "");
    return std::make_shared<ReceivedDeviceMessage>(
        message_metadata.contextToken, node, message_metadata.total_row_size);
  }

  DefineClassName(ComponentMessage);
};

void ExecMaster(){
 // Create server
  std::unique_ptr<Server> server = Server::BatchProcessing(8000);

  // Configure server
  auto endpoint = ComponentMessage::MessageID();
  server->registerEndPoint(endpoint);
  server->registerContext(context_token);

  server->registerDeviceDeserializerForEndPoint(ComponentMessage::MakeFrom, endpoint);

  // Run server in a thread
  server->Run();
  std::this_thread::sleep_for(std::chrono::seconds(1));

  std::thread([&]() {
    std::shared_ptr<ReceivedMessage> message;
    while (message = server->getMessage(context_token, endpoint) ) {
      auto mock_message = std::dynamic_pointer_cast<ComponentMessage>(message);
      // mockFlag.Flag();
    }
    server->Close();
  }).join();
}
void ExecWorker(){

  // Create message
  auto node = std::make_shared<Node>(Address::TCP("localhost", 8000, 9999), "");
  ComponentMessage message{context_token, *node, 0};

  auto client = blazingdb::transport::ClientTCP::Make("localhost", 8000);
  size_t number_of_samples = 10;
  for (size_t i = 0; i < number_of_samples; i++) {
    try {
      auto status = client->Send(message);
      EXPECT_TRUE(status.IsOk());
    } catch (std::exception &e) {
      FAIL() << e.what();
    }
  }
  client->notifyLastMessageEvent(message.metadata());
}

// TEST(SendInBatches, MasterAndWorker) {
//  if (fork() > 0) {
//    ExecMaster();
//  } else {
//    ExecWorker();
//  }
// }

// TEST(SendInBatches, Master) { ExecMaster(); }
// TEST(SendInBatches, Worker) { ExecWorker(); }

}  // namespace transport
}  // namespace blazingdb
