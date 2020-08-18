#include "tests/utilities/BlazingUnitTest.h"
#include "communication/CommunicationInterface/messageReceiver.hpp"
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

  comm::message_receiver msg_rcv(column_transports, metadata, output_cache);
  for (size_t i = 0; i < raw_buffers.size(); i++) {
    msg_rcv.set_buffer_size(i,buffer_sizes[i]);
    cudaMemcpy(msg_rcv.get_buffer(i),gpu_buffers[i]->data(),buffer_sizes[i],cudaMemcpyDeviceToDevice);
  }

  msg_rcv.finish();

  auto pulled_table = output_cache->pullFromCache();

  cudf::test::expect_tables_equal(orig_table, pulled_table->view());
}
