
#include <blazingdb/manager/Manager.h>
#include <blazingdb/manager/NodeDataMessage.h>

//
//  Hello World server in C++
//  Binds REP socket to tcp://*:5555
//  Expects "Hello" from client, replies with "World"
//
#include <zmq.hpp>
#include <string>
#include <iostream>
#include <gtest/gtest.h>
#include <thread>
namespace blazingdb {
namespace manager {

TEST(TestManager, ConnectionAndGenerateContext) {
  using Status = blazingdb::transport::Status;

  std::unique_ptr<Manager> manager = Manager::MakeServer(9999);
  int accessToken = 1234;
  manager->Run();

  std::this_thread::sleep_for(std::chrono::seconds(1));

  // Register node
  auto address = transport::Address::TCP("localhost", 9999, 1234);
  std::shared_ptr<Node> node = Node::Make(address);

  // Create message
  try {
    auto client = Manager::MakeClient("localhost", 9999);

    auto message = NodeDataMessage::Make(node);
    std::cout << "sending node:";
    node->print();
    const Status status = client->Send(*message);
    EXPECT_TRUE(status.IsOk());
  } catch (const std::exception &error) {
    FAIL() << error.what();
  }

  std::this_thread::sleep_for(std::chrono::seconds(1));

  EXPECT_EQ(manager->getCluster().getTotalNodes(), 1);

  Context *context =
      manager->generateContext("plan", 99, 1);

  EXPECT_EQ("plan", context->getLogicalPlan());

  manager->Close();
}

void ExecWorker(std::shared_ptr<transport::Address> &address){
  using Status = blazingdb::transport::Status;
  std::this_thread::sleep_for(std::chrono::seconds(1));

  // Register node
  try {
    // Create message
    std::cout << "sending node:";
    std::shared_ptr<Node> node = Node::Make(address);
    node->print();
    auto message = NodeDataMessage::Make(node);
    auto client = Manager::MakeClient(address->metadata().ip, address->metadata().comunication_port);
    const Status status = client->Send(*message);
    EXPECT_TRUE(status.IsOk());
  } catch (const std::exception &error) {
    FAIL() << error.what();
  }
}

TEST(TestManager, Test2){
  auto address = transport::Address::TCP("localhost", 9999, 1234);

  std::unique_ptr<Manager> manager = Manager::MakeServer(9999);
  manager->Run();

  ExecWorker(address);

  EXPECT_EQ(manager->getCluster().getTotalNodes(), 1);
  Context *context =
      manager->generateContext("plan", 99, 1);
  EXPECT_EQ("plan", context->getLogicalPlan());
  auto nodes = context->getAllNodes();
  std::cout << "getAllNodes:\n";
  for (auto node : nodes) {
    node->print();
  }
}
//
//TEST(TestManager, Server) {
//  void* context = zmq_ctx_new();
//  void* respond = zmq_socket(context, ZMQ_REP);
//  zmq_bind(respond, "tcp://*:4040");
//
//  printf("Starting...\n");
//
//  for(;;) {
//
//    zmq_msg_t request;
//    zmq_msg_init(&request);
//    zmq_msg_recv(&request, respond, 0);
//    printf("Received: hello\n");
//
//    zmq_msg_close(&request);
//
//    sleep(1);
//
//    zmq_msg_t msg1, msg2, msg3, msg4, msg5;
//    zmq_msg_init_size(&msg1, 2);
//    zmq_msg_init_size(&msg2, 2);
//    zmq_msg_init_size(&msg3, 2);
//    zmq_msg_init_size(&msg4, 2);
//    zmq_msg_init_size(&msg5, 2);
//
//    memcpy(zmq_msg_data(&msg1), "w", 2);
//    zmq_msg_send(&msg1, respond, ZMQ_SNDMORE);
//
//
//    memcpy(zmq_msg_data(&msg2), "o", 2);
//    zmq_msg_send(&msg2, respond, ZMQ_SNDMORE);
//
//    memcpy(zmq_msg_data(&msg3), "r", 2);
//    zmq_msg_send(&msg3, respond, ZMQ_SNDMORE);
//
//    memcpy(zmq_msg_data(&msg4), "l", 2);
//    zmq_msg_send(&msg4, respond, ZMQ_SNDMORE);
//
//    memcpy(zmq_msg_data(&msg5), "d", 2);
//    zmq_msg_send(&msg5, respond, 0);
//
//
//    zmq_msg_close(&msg1);
//    zmq_msg_close(&msg2);
//    zmq_msg_close(&msg3);
//    zmq_msg_close(&msg4);
//    zmq_msg_close(&msg5);
//  }
//
//  zmq_close(respond);
//  zmq_ctx_destroy(context);
//
//}
//
//TEST(TestManager, Client) {
//
//  void* context = zmq_ctx_new();
//
//  printf("Client Starting....\n");
//
//  void* request = zmq_socket(context, ZMQ_REQ);
//  zmq_connect(request, "tcp://localhost:4040");
//  zmq_send(request, "hello", 5, 0);
//
//  for(;;) {
//
//    zmq_msg_t reply;
//    zmq_msg_init(&reply);
//    zmq_msg_recv(&reply, request, 0);
//    printf("Received: %s\n", (char *) zmq_msg_data(&reply));
//    zmq_msg_close(&reply);
//
//    uint64_t more_part;
//    size_t more_size = sizeof(more_part);
//    zmq_getsockopt(request, ZMQ_RCVMORE, &more_part, &more_size);
//    if (!more_part)
//      break;
//
//  }
//
//  zmq_close(request);
//  zmq_ctx_destroy(context);
//
//}
//
//TEST(TestManager, Server) {
//    //  Prepare our context and socket
//    zmq::context_t context (1);
//    zmq::socket_t receiver (context, ZMQ_PULL);
//    receiver.bind ("tcp://*:5558");
//    int request_nbr = 0;
//    while (true) {
//      std::string string(2048*2048, 'a' + request_nbr % 10);
//      zmq::message_t message;
//      receiver.recv(&message);
//      std::string smessage(static_cast<char*>(message.data()), message.size());
//      assert(smessage == string);
//      std::cout << smessage.substr(0, 2) << std::endl;
//      request_nbr++;
//    }
//}
//
//
//TEST(TestManager, Client) {
//  //  Socket to send messages to
//  //  Do 10 requests, waiting each time for a response
//  zmq::context_t context(1);
//  zmq::socket_t sender(context, ZMQ_PUSH);
//  sender.connect("tcp://localhost:5558");
//  std::mutex muted;
//  for (int request_nbr = 0; request_nbr != 8; request_nbr++) {
//    std::thread([&](){
//      std::string string(2048*2048, 'a' + request_nbr % 10);
//      std::cout << "sending: " << string.substr(0, 2) << std::endl;
//      zmq::message_t message(string.size());
//      memcpy (message.data(), string.data(), string.size());
////      std::lock_guard<std::mutex> loc(muted);
//      sender.send(message);
//    }).join();
//  }
//}


} // namespace transport
} // namespace blazingdb
