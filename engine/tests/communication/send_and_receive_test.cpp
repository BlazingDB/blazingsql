#include "tests/utilities/BlazingUnitTest.h"
#include "src/communication/CommunicationInterface/messageReceiver.hpp"
#include "src/communication/CommunicationInterface/messageSender.hpp"
#include "src/communication/CommunicationInterface/messageListener.hpp"
#include <src/execution_graph/logic_controllers/CacheMachine.h>


#include <memory>
#include <cudf_test/base_fixture.hpp>
#include <cudf/table/table_view.hpp>
#include <cudf/column/column_factories.hpp>
#include <cudf_test/column_utilities.hpp>
#include <cudf_test/type_lists.hpp>
#include <cudf_test/column_wrapper.hpp>
#include <cudf_test/table_utilities.hpp>

using namespace ral::frame;
using namespace ral::cache;

struct SendAndReceiveTest : public BlazingUnitTest {
  SendAndReceiveTest() {
    ral::memory::set_allocation_pools(4000000, 10,
										4000000, 10, false,nullptr);

		blazing_host_memory_resource::getInstance().initialize(0.5);
  }

  ~SendAndReceiveTest() {
    ral::memory::empty_pools();
  }
};


template<class TypeParam>
std::unique_ptr<cudf::column> make_col(cudf::size_type size) {
    auto sequence = cudf::test::make_counting_transform_iterator(0, [](auto i) { return TypeParam(i); });
    std::vector<TypeParam> data(sequence, sequence + size);
    cudf::test::fixed_width_column_wrapper<TypeParam> col(data.begin(), data.end());
    return col.release();
}

std::unique_ptr<ral::frame::BlazingTable> build_custom_table() {
    
    
	cudf::size_type size = 10;

	auto num_column_1 = make_col<int32_t>(size);
	auto num_column_2 = make_col<int64_t>(size);
	auto num_column_3 = make_col<float>(size);
	auto num_column_4 = make_col<double>(size);

	std::vector<std::unique_ptr<cudf::column>> columns;
	columns.push_back(std::move(num_column_1));
	columns.push_back(std::move(num_column_2));
	columns.push_back(std::move(num_column_3));
	columns.push_back(std::move(num_column_4));

	cudf::test::strings_column_wrapper col2({"d", "e", "a", "d", "k", "d", "l", "a", "b", "c"}, {1, 0, 1, 1, 1, 1, 1, 1, 0, 1});

	std::unique_ptr<cudf::column> str_col = std::make_unique<cudf::column>(std::move(col2));
	columns.push_back(std::move(str_col));

	std::vector<std::string> column_names = {"INT64", "INT32", "FLOAT64", "FLOAT32", "STRING"};

	auto table = std::make_unique<cudf::table>(std::move(columns));
	return std::make_unique<ral::frame::BlazingTable>(ral::frame::BlazingTable(std::move(table), column_names));
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



std::shared_ptr<ral::cache::CacheMachine> setup_sender(){

  uint16_t ralId = 1;
  std::string worker_id = "1";
  std::string ip = "127.0.0.1";
  std::int32_t port = 8890;
  std::map<std::string, comm::node> nodes_info_map;
  nodes_info_map.emplace(worker_id, comm::node(ralId, worker_id, ip, port));

  ucp_context_h ucp_context = nullptr;
  std::shared_ptr<ral::cache::CacheMachine> output_cache = std::make_shared<CacheMachine>(nullptr, "messages_out", false,CACHE_LEVEL_CPU );

  comm::blazing_protocol protocol = comm::blazing_protocol::tcp;
  bool require_acknowledge = false;
  ucp_worker_h self_worker = nullptr;
  int num_comm_threads = 20;
  comm::message_sender::initialize_instance(output_cache,
			nodes_info_map,
			num_comm_threads, ucp_context, self_worker, ralId,protocol,require_acknowledge);
  comm::message_sender::get_instance()->run_polling();

  return comm::message_sender::get_instance()->get_output_cache();

}


std::shared_ptr<ral::cache::CacheMachine> setup_receiver() {

  uint16_t ralId = 0;
  std::string worker_id = "0";
  std::string ip = "127.0.0.1";
  std::int32_t port = 8890;
  std::map<std::string, comm::node> nodes_info_map;
  nodes_info_map.emplace(worker_id, comm::node(ralId, worker_id, ip, port));

  std::shared_ptr<ral::cache::CacheMachine> input_cache = std::make_shared<CacheMachine>(nullptr, "messages_in", false);

  int num_comm_threads = 20;
  comm::tcp_message_listener::initialize_message_listener(nodes_info_map,port,num_comm_threads, input_cache);
  comm::tcp_message_listener::get_instance()->start_polling();

  return comm::tcp_message_listener::get_instance()->get_input_cache(); 
  
}


TEST_F(SendAndReceiveTest, SendAndReceiveTest0) {

  std::shared_ptr<ral::cache::CacheMachine> output_cache = setup_sender();
  std::shared_ptr<ral::cache::CacheMachine> input_cache = setup_receiver();
  
  auto table = build_custom_table();

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

  auto received_table = input_cache->pullFromCache();

  auto table_for_comparison = build_custom_table();
  cudf::test::expect_tables_equivalent(table_for_comparison->view(), received_table->view());	

}




TEST_F(SendAndReceiveTest, SendAndReceiveALOT) {

  std::shared_ptr<ral::cache::CacheMachine> output_cache = setup_sender();
  std::shared_ptr<ral::cache::CacheMachine> input_cache = setup_receiver();

  int num_send = 20;
  cudf::size_type big_table_size = 1000000;

  std::vector<std::thread> send_threads;
  for (int i = 0; i < num_send; i++){
    send_threads.push_back(std::thread([output_cache, big_table_size, i]{
      
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

  std::vector<std::thread> receive_threads;
  auto table_for_comparison = build_big_table(big_table_size);
  for (int i = 0; i < num_send; i++){
    receive_threads.push_back(std::thread([input_cache, &table_for_comparison,i]{

      auto received_table = input_cache->pullFromCache();
      std::cout<<"input_cache->pullFromCache() "<<i<<std::endl;

  
      cudf::test::expect_tables_equivalent(table_for_comparison->view(), received_table->view());	
      }));
  }

  for (int i = 0; i < num_send; i++){ 
    send_threads[i].join();
  }
  for (int i = 0; i < num_send; i++){ 
    receive_threads[i].join();
  }

  std::cout<<"FIRST BATCH COMPLETE"<<std::endl;


  send_threads.resize(0);
  for (int i = 0; i < num_send; i++){
    send_threads.push_back(std::thread([output_cache, big_table_size]{
        
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
    }));
  }

  receive_threads.resize(0);
  for (int i = 0; i < num_send; i++){
    receive_threads.push_back(std::thread([input_cache, &table_for_comparison]{

      auto received_table = input_cache->pullFromCache();
  
      cudf::test::expect_tables_equivalent(table_for_comparison->view(), received_table->view());	
      }));
  }

  for (int i = 0; i < num_send; i++){ 
    send_threads[i].join();
  }
  for (int i = 0; i < num_send; i++){ 
    receive_threads[i].join();
  }

  std::cout<<"SECOND BATCH COMPLETE"<<std::endl;
  
}

