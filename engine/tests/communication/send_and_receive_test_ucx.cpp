#include "tests/utilities/BlazingUnitTest.h"
#include "src/communication/CommunicationInterface/messageReceiver.hpp"
#include "src/communication/CommunicationInterface/messageSender.hpp"
#include "src/communication/CommunicationInterface/messageListener.hpp"
#include <src/execution_graph/logic_controllers/CacheMachine.h>
#include <src/communication/ucx_init.h>


#include <memory>
#include <cudf_test/base_fixture.hpp>
#include <cudf/table/table_view.hpp>
#include <cudf/column/column_factories.hpp>
#include <cudf_test/column_utilities.hpp>
#include <cudf_test/type_lists.hpp>
#include <cudf_test/column_wrapper.hpp>
#include <cudf_test/table_utilities.hpp>


#include <ucp/api/ucp.h>
#include <ucs/type/status.h>
#include <ucs/memory/memory_type.h>

using namespace ral::frame;
using namespace ral::cache;
using namespace ral::communication;

struct SendAndReceiveTestUCX : public BlazingUnitTest {
  // SendAndReceiveTestUCX() {
  //   ral::memory::set_allocation_pools(4000000, 10,
	// 									4000000, 10, false,nullptr);

	// 	blazing_host_memory_resource::getInstance().initialize(0.5);
  // }

  // ~SendAndReceiveTestUCX() {
  //   ral::memory::empty_pools();
  // }
};

const std::uint16_t exchangingPort = 13337;
const int num_comm_threads = 1;
const int num_send = 20;
const cudf::size_type big_table_size = 1000000;


template<class TypeParam>
std::unique_ptr<cudf::column> make_col(cudf::size_type size) {
    auto sequence = cudf::test::make_counting_transform_iterator(0, [](auto i) { return TypeParam(i); });
    std::vector<TypeParam> data(sequence, sequence + size);
    cudf::test::fixed_width_column_wrapper<TypeParam> col(data.begin(), data.end());
    return col.release();
}

std::unique_ptr<ral::frame::BlazingTable> build_big_table(cudf::size_type size) {
    
	auto num_column_1 = make_col<int32_t>(size);
	auto num_column_2 = make_col<int64_t>(size);
	auto num_column_3 = make_col<float>(size);
	auto num_column_4 = make_col<double>(size);

	std::vector<std::unique_ptr<cudf::column>> columns;
	columns.push_back(std::move(num_column_1));
	columns.push_back(std::move(num_column_2));
	columns.push_back(std::move(num_column_3));
	columns.push_back(std::move(num_column_4));

	std::vector<std::string> column_names = {"INT64", "INT32", "FLOAT64", "FLOAT32"};

	auto table = std::make_unique<cudf::table>(std::move(columns));
	return std::make_unique<ral::frame::BlazingTable>(ral::frame::BlazingTable(std::move(table), column_names));
}



void SenderCall(const UcpWorkerAddress &peerUcpWorkerAddress,
                ucp_worker_h ucp_worker,
                ucp_context_h ucp_context,
                const std::size_t requestSize,
                const std::size_t testStringLength) {
  
  uint16_t self_ralId = 0;
  uint16_t other_ralId = 1;

  ucp_ep_h ucp_ep = CreateUcpEp(ucp_worker, peerUcpWorkerAddress);

  std::map<std::string, comm::node> nodes_info_map;
  nodes_info_map.emplace("server", comm::node(other_ralId, "server", ucp_ep, ucp_worker));

  std::shared_ptr<ral::cache::CacheMachine> output_cache = std::make_shared<CacheMachine>(nullptr, "messages_out", false,CACHE_LEVEL_CPU );
  
  bool require_acknowledge = false;
  comm::message_sender::initialize_instance(output_cache, nodes_info_map, num_comm_threads, ucp_context, 
    ucp_worker, self_ralId, comm::blazing_protocol::ucx, require_acknowledge);
  comm::message_sender::get_instance()->run_polling();

  output_cache = comm::message_sender::get_instance()->get_output_cache();

  auto progress_manager = comm::ucp_progress_manager::get_instance(ucp_worker, requestSize);

  std::this_thread::sleep_for(std::chrono::seconds(5));

  std::vector<std::thread> send_threads;
  for (int i = 0; i < num_send; i++){
    send_threads.push_back(std::thread([output_cache, big_table_size, i]{

      cudaSetDevice(0);
      
      auto table = build_big_table(big_table_size);

      ral::cache::MetadataDictionary metadata;
      metadata.add_value(ral::cache::RAL_ID_METADATA_LABEL,1);
      metadata.add_value(ral::cache::KERNEL_ID_METADATA_LABEL, 0);
      metadata.add_value(ral::cache::QUERY_ID_METADATA_LABEL, 0);
      metadata.add_value(ral::cache::ADD_TO_SPECIFIC_CACHE_METADATA_LABEL, "false");
      metadata.add_value(ral::cache::CACHE_ID_METADATA_LABEL, "");
      metadata.add_value(ral::cache::SENDER_WORKER_ID_METADATA_LABEL, 0);
      metadata.add_value(ral::cache::WORKER_IDS_METADATA_LABEL, "1");
      metadata.add_value(ral::cache::UNIQUE_MESSAGE_ID, std::to_string(1));

      const std::string MESSAGE_ID_CONTENT = metadata.get_values()[ral::cache::QUERY_ID_METADATA_LABEL] + "_" +
                                              metadata.get_values()[ral::cache::KERNEL_ID_METADATA_LABEL] + "_" +
                                              metadata.get_values()[ral::cache::SENDER_WORKER_ID_METADATA_LABEL];

      metadata.add_value(ral::cache::MESSAGE_ID, MESSAGE_ID_CONTENT);
        
      output_cache->addToCache(std::move(table),"",true,metadata,true, true);
      std::cout<<"output_cache->addToCache "<<i<<std::endl;
    }));
  }

  for (int i = 0; i < num_send; i++){ 
    send_threads[i].join();
  }

  std::this_thread::sleep_for(std::chrono::seconds(15));
}

void ReceiverCall(const UcpWorkerAddress &peerUcpWorkerAddress,
                  ucp_worker_h ucp_worker,
                  ucp_context_h ucp_context,
                  const std::size_t requestSize,
                  const std::size_t testStringLength) {
  
  uint16_t self_ralId = 1;
  uint16_t other_ralId = 0;

  ucp_ep_h ucp_ep = CreateUcpEp(ucp_worker, peerUcpWorkerAddress);

  std::map<std::string, comm::node> nodes_info_map;
  nodes_info_map.emplace("client", comm::node(other_ralId, "client", ucp_ep, ucp_worker));

  std::shared_ptr<ral::cache::CacheMachine> input_cache = std::make_shared<CacheMachine>(nullptr, "messages_in", false);

  comm::ucx_message_listener::initialize_message_listener(
    ucp_context, ucp_worker, nodes_info_map,num_comm_threads, input_cache);

  comm::ucx_message_listener::get_instance()->poll_begin_message_tag(true);

  std::this_thread::sleep_for(std::chrono::seconds(5));

  input_cache = comm::ucx_message_listener::get_instance()->get_input_cache(); 

  std::vector<std::thread> receive_threads;
  auto table_for_comparison = build_big_table(big_table_size);
  for (int i = 0; i < num_send; i++){
    receive_threads.push_back(std::thread([input_cache, &table_for_comparison,i]{

      cudaSetDevice(0);
      auto received_table = input_cache->pullFromCache();
      std::cout<<"input_cache->pullFromCache() "<<i<<std::endl;

  
      cudf::test::expect_tables_equivalent(table_for_comparison->view(), received_table->view());	
      }));
  }

  for (int i = 0; i < num_send; i++){ 
    receive_threads[i].join();
  }

  std::this_thread::sleep_for(std::chrono::seconds(15));
  
}



template <class Callback>
void Run(Callback &&callback,
         std::unique_ptr<AddressExchanger> &&addressExchanger) {
           cudaSetDevice(0);
  std::cout<<"run 0"<<std::endl;
  ucp_context_h ucp_context = CreateUcpContext();
  std::cout<<"run 1"<<std::endl;
  ucp_worker_h ucp_worker = CreatetUcpWorker(ucp_context);
  std::cout<<"run 2"<<std::endl;
  UcpWorkerAddress ucpWorkerAddress = GetUcpWorkerAddress(ucp_worker);
  std::cout<<"run 3"<<std::endl;
  UcpWorkerAddress peerUcpWorkerAddress =
      addressExchanger->Exchange(ucpWorkerAddress);

      std::cout<<"run 4"<<std::endl;

  std::cout << '[' << std::hex << std::this_thread::get_id()
            << "] local: " << std::hex
            << *reinterpret_cast<std::size_t *>(ucpWorkerAddress.address) << ' '
            << ucpWorkerAddress.length << std::endl
            << '[' << std::hex << std::this_thread::get_id()
            << "] peer: " << std::hex
            << *reinterpret_cast<std::size_t *>(peerUcpWorkerAddress.address)
            << ' ' << peerUcpWorkerAddress.length << std::endl;

  ucp_context_attr_t attr;
  attr.field_mask = UCP_ATTR_FIELD_REQUEST_SIZE;

  ucs_status_t status = ucp_context_query(ucp_context, &attr);
  std::cout<<"run 5"<<std::endl;
  CheckError(status != UCS_OK, "ucp_context_query");

  static const std::size_t testStringLength = 10;
  callback(
      peerUcpWorkerAddress, ucp_worker, ucp_context, attr.request_size, testStringLength);

  ucp_worker_release_address(ucp_worker, ucpWorkerAddress.address);
  ucp_worker_destroy(ucp_worker);
  ucp_cleanup(ucp_context);
}


TEST_F(SendAndReceiveTestUCX, send_receive_test) {

  int pid = fork();

  if (pid == 0){
    ::Run(SenderCall, AddressExchanger::MakeForSender(exchangingPort));
  } else if (pid > 0) {
    ::Run(ReceiverCall, AddressExchanger::MakeForReceiver(exchangingPort, "192.168.1.107"));
  } else {
    std::cout<<"error forking"<<std::endl;
  }

  // std::thread sender = std::thread([]{
  //   cudaSetDevice(0);
  //   ::Run(SenderCall, AddressExchanger::MakeForSender(exchangingPort));
  // });
  // std::thread receiver = std::thread([]{
  //   cudaSetDevice(0);
  //   ::Run(ReceiverCall, AddressExchanger::MakeForReceiver(exchangingPort, "localhost"));
  // });
  // sender.join();
  // receiver.join();
}


