#include "tests/utilities/BlazingUnitTest.h"
#include "communication/CommunicationInterface/messageReceiver.hpp"
#include "communication/CommunicationInterface/bufferTransport.hpp"
#include "execution_graph/logic_controllers/CacheMachine.h"

#include <memory>
#include <tests/utilities/base_fixture.hpp>
#include <cudf/table/table_view.hpp>
#include <cudf/sorting.hpp>
#include <cudf/copying.hpp>
#include <cudf/column/column_factories.hpp>
#include <tests/utilities/column_utilities.hpp>
#include <tests/utilities/type_lists.hpp>
#include <tests/utilities/column_wrapper.hpp>
#include <tests/utilities/table_utilities.hpp>

using namespace ral::frame;

struct MessageReceiverTest : public BlazingUnitTest {
  MessageReceiverTest() {}

  ~MessageReceiverTest() {}
};

TEST_F(MessageReceiverTest, receive_test) {
  cudf::test::fixed_width_column_wrapper<int> col1{{4, 5, 3, 5, 8, 5, 6}};
  cudf::table_view orig_table{{col1}};

  std::vector<std::string> columnNames = {"column_1"};

  std::vector<std::size_t> buffer_sizes;
  std::vector<const char *> raw_buffers;
  std::vector<ColumnTransport> column_transports;
  std::vector<std::unique_ptr<rmm::device_buffer>> temp_scope_holder;
  std::tie(buffer_sizes, raw_buffers, column_transports, temp_scope_holder) = comm::serialize_gpu_message_to_gpu_containers(ral::frame::BlazingTableView{orig_table, columnNames});

  std::vector<std::unique_ptr<rmm::device_buffer>> gpu_buffers(raw_buffers.size());
  for (size_t i = 0; i < raw_buffers.size(); i++) {
    gpu_buffers[i] = std::make_unique<rmm::device_buffer>(raw_buffers[i], buffer_sizes[i]);
  }

  ral::cache::MetadataDictionary metadata;
  auto output_cache = std::make_shared<ral::cache::CacheMachine>(nullptr);
  
  auto meta_buffer = detail::serialize_metadata_and_transports_and_buffer_sizes(metadata,
                    column_transports,
                    buffer_sizes);
  comm::message_receiver msg_rcv(meta_buffer);
  for (size_t i = 0; i < raw_buffers.size(); i++) {
    msg_rcv.allocate_buffer(i);
    cudaMemcpy(msg_rcv.get_buffer(i),gpu_buffers[i]->data(),buffer_sizes[i],cudaMemcpyDeviceToDevice);
  }

  msg_rcv.finish();

  auto pulled_table = output_cache->pullFromCache();

  cudf::test::expect_tables_equal(orig_table, pulled_table->view());
}

TEST_F(MessageReceiverTest, receive_metatdata) {
  cudf::test::fixed_width_column_wrapper<int> col1{{4, 5, 3, 5, 8, 5, 6}};
  cudf::table_view orig_table{{col1}};

  std::vector<std::string> columnNames = {"column_1"};

  std::vector<std::size_t> buffer_sizes;
  std::vector<const char *> raw_buffers;
  std::vector<ColumnTransport> column_transports;
  std::vector<std::unique_ptr<rmm::device_buffer>> temp_scope_holder;
  std::tie(buffer_sizes, raw_buffers, column_transports, temp_scope_holder) = comm::serialize_gpu_message_to_gpu_containers(ral::frame::BlazingTableView{orig_table, columnNames});

  ral::cache::MetadataDictionary metadata;
  metadata.add_value(ral::cache::KERNEL_ID_METADATA_LABEL, 0);
  metadata.add_value(ral::cache::QUERY_ID_METADATA_LABEL, 0);
  metadata.add_value(ral::cache::ADD_TO_SPECIFIC_CACHE_METADATA_LABEL, "false");
  metadata.add_value(ral::cache::CACHE_ID_METADATA_LABEL, "");
  metadata.add_value(ral::cache::SENDER_WORKER_ID_METADATA_LABEL, "ucx://127.0.0.1");
  metadata.add_value(ral::cache::WORKER_IDS_METADATA_LABEL, "ucx://127.0.0.2");
  metadata.add_value(ral::cache::MESSAGE_ID, "");

  auto bytes_buffer = comm::detail::serialize_metadata_and_transports(metadata, column_transports);

  ral::cache::MetadataDictionary out_metadata;
  std::vector<ColumnTransport> out_column_transports;
  std::vector<size_t> buffer_sizes;
  std::tie(out_metadata, out_column_transports, buffer_sizes) = comm::detail::get_metadata_and_transports_and_buffer_sizes_from_bytes(bytes_buffer);

  auto expected_values = metadata.get_values();
  auto out_values = out_metadata.get_values();

  EXPECT_TRUE(expected_values == out_values);
}
