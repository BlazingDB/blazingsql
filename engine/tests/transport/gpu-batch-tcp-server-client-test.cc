#include <spdlog/spdlog.h>
#include <spdlog/async.h>
#include <spdlog/sinks/basic_file_sink.h>
#include <spdlog/sinks/stdout_color_sinks.h>

#include "utils/column_factory.h"
#include <blazingdb/transport/Node.h>
#include <blazingdb/transport/api.h>
#include <blazingdb/transport/io/reader_writer.h>

#include "communication/messages/ComponentMessages.h"
#include "communication/network/Client.h"
#include "communication/network/Server.h"
#include <from_cudf/cpp_tests/utilities/base_fixture.hpp>
#include <from_cudf/cpp_tests/utilities/column_wrapper.hpp>
#include <from_cudf/cpp_tests/utilities/table_utilities.hpp>
#include <execution_graph/logic_controllers/BatchProcessing.h>
#include "../BlazingUnitTest.h"

using ral::communication::messages::SampleToNodeMasterMessage;
using ral::communication::messages::ReceivedDeviceMessage;
using ral::communication::messages::ReceivedHostMessage;

using ral::communication::network::Client;
using ral::communication::network::Node;
using ral::communication::network::Server;
using Address = blazingdb::transport::Address;
using GPUMessage = blazingdb::transport::GPUMessage;

constexpr uint32_t context_token = 3465;


// TODO get GPU_MEMORY_SIZE
auto GPU_MEMORY_SIZE = 4096;


// Helper function to compare two floating-point column contents
template <typename T>
void expect_column_data_equal(std::vector<T> const& lhs,
							  cudf::column_view const& rhs) {
  EXPECT_THAT(cudf::test::to_host<T>(rhs).first, lhs);
}


void ExecMaster() {
	cuInit(0);
	// Run server
	Server::start(8000, true);

	auto sizeBuffer = GPU_MEMORY_SIZE / 4;
	blazingdb::transport::io::setPinnedBufferProvider(sizeBuffer, 1);
	Server::getInstance().registerContext(context_token);
	auto cache_machine = ral::cache::create_cache_machine(ral::cache::cache_settings{.type = ral::cache::CacheType::SIMPLE});
	// auto cache_machine = std::make_shared<ral::cache::HostCacheMachine>();

	std::string message_token = SampleToNodeMasterMessage::MessageID() + "_" + std::to_string(1);
	
	BlazingThread([cache_machine]() {
		auto table = cache_machine->pullFromCache();
		assert(table != nullptr);
		// auto table = ral::communication::messages::deserialize_from_cpu(host_table.get());
		std::cout << "message received\n";
		expect_column_data_equal(std::vector<int32_t>{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}, table->view().column(0));
		cudf::test::strings_column_wrapper expected({"d", "e", "a", "d", "k", "d", "l", "a", "b", "c"}, {1, 0, 1, 1, 1, 1, 1, 1, 0 , 1});
		cudf::test::expect_columns_equal(table->view().column(4), expected);		
		std::this_thread::sleep_for (std::chrono::seconds(1));
	}).join();
}

void ExecWorker() {
	cuInit(0);
	// todo get GPU_MEMORY_SIZE
	auto sizeBuffer = GPU_MEMORY_SIZE / 4;
	auto nthread = 4;
	blazingdb::transport::io::setPinnedBufferProvider(sizeBuffer, nthread);
	auto sender_node = Node(Address::TCP("127.0.0.1", 8001, 1234));
	auto server_node = Node(Address::TCP("127.0.0.1", 8000, 1234));

	const auto samples = blazingdb::test::build_custom_table();
	
	std::uint64_t total_row_size = samples.num_rows();
	ral::frame::BlazingTableView table_view(samples.view(), samples.names());
	std::string message_token = SampleToNodeMasterMessage::MessageID() + "_" + std::to_string(1);
	auto message = std::make_shared<SampleToNodeMasterMessage>(message_token, context_token, sender_node, table_view, total_row_size);

	Client::send(server_node, *message);
	Client::notifyLastMessageEvent(server_node, message->metadata());
}


struct SendBatchSamplesTest : public BlazingUnitTest {

};


// TO use in separate process by:
// ./blazingdb-communication-gtest --gtest_filter=SendBatchSamplesTest.Master
// TEST_F(SendBatchSamplesTest, Master) {
//    ExecMaster();
//  }


// //  // ./blazingdb-communication-gtest --gtest_filter=SendBatchSamplesTest.Worker
// TEST_F(SendBatchSamplesTest, Worker) {
//    ExecWorker();
//  }
