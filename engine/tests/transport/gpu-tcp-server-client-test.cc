#include "cudf/types.h"
#include "utils/column_factory.h"
#include <blazingdb/transport/Node.h>
#include <blazingdb/transport/api.h>
#include <blazingdb/transport/io/reader_writer.h>
#include <chrono>
#include <cuda.h>

#include "Utils.cuh"
#include "communication/messages/ComponentMessages.h"
#include "communication/messages/GPUComponentMessage.h"
#include "communication/network/Client.h"
#include "communication/network/Server.h"
#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <memory>
#include <numeric>
#include <nvstrings/NVCategory.h>
#include <thread>

using ral::communication::messages::SampleToNodeMasterMessage;
using ral::communication::network::Client;
using ral::communication::network::Node;
using ral::communication::network::Server;
using Address = blazingdb::transport::Address;
using GPUMessage = blazingdb::transport::GPUMessage;

constexpr uint32_t context_token = 3465;


std::shared_ptr<GPUMessage> CreateSampleToNodeMaster(uint32_t context_token, std::shared_ptr<Node> & sender_node) {
	std::vector<gdf_column_cpp> samples = blazingdb::test::build_table();

	for(auto & col : samples) {
		print_gdf_column(col.get_gdf_column());
	}
	std::uint64_t total_row_size = samples[0].size();
	std::string message_token = SampleToNodeMasterMessage::MessageID() + "_" + std::to_string(1);
	return std::make_shared<SampleToNodeMasterMessage>(
		message_token, context_token, sender_node, samples, total_row_size);
}

// TODO get GPU_MEMORY_SIZE
auto GPU_MEMORY_SIZE = 4096;

static void ExecMaster() {
	cuInit(0);
	rmmInitialize(nullptr);
	// Run server
	Server::start(8000);

	auto sizeBuffer = GPU_MEMORY_SIZE / 4;
	blazingdb::transport::io::setPinnedBufferProvider(sizeBuffer, 1);
	Server::getInstance().registerContext(context_token);
	std::thread([]() {
		std::string message_token = SampleToNodeMasterMessage::MessageID() + "_" + std::to_string(1);

		auto message = Server::getInstance().getMessage(context_token, message_token);
		auto concreteMessage = std::static_pointer_cast<SampleToNodeMasterMessage>(message);
		std::cout << "message received\n";
		for(gdf_column_cpp & column : concreteMessage->getSamples()) {
			print_gdf_column(column.get_gdf_column());
		}
		Server::getInstance().close();
	}).join();
}

static void ExecWorker() {
	cuInit(0);
	rmmInitialize(nullptr);
	// todo get GPU_MEMORY_SIZE
	auto sizeBuffer = GPU_MEMORY_SIZE / 4;
	auto nthread = 4;
	blazingdb::transport::io::setPinnedBufferProvider(sizeBuffer, nthread);
	// This lines are not necessary!!
	//  RalServer::start(8001);
	//  RalServer::getInstance().registerContext(context_token);

	auto sender_node = std::make_shared<Node>(Address::TCP("127.0.0.1", 8001, 1234));
	auto server_node = std::make_shared<Node>(Address::TCP("127.0.0.1", 8000, 1234));

	auto message = CreateSampleToNodeMaster(context_token, sender_node);
	Client::send(*server_node, *message);
}
// TODO: move common code of TCP client and server to blazingdb::network in order to be shared by manager and transport
// TODO: check when the ip, port is busy, return exception!
// TODO: check when the message is not registered, or the wrong message is registered
TEST(SendSamplesTest, MasterAndWorker) {
	if(fork() > 0) {
		ExecMaster();
	} else {
		ExecWorker();
	}
}

// // TO use in separate process by:
// // ./blazingdb-communication-gtest --gtest_filter=SendSamplesTest.Master
// TEST(SendSamplesTest, Master) {
//   ExecMaster();
// }
// // ./blazingdb-communication-gtest --gtest_filter=SendSamplesTest.Worker
// TEST(SendSamplesTest, Worker) {
//   ExecWorker();
// }
