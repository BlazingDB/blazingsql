#include <blazingdb/transport/Node.h>
#include <blazingdb/transport/api.h>
#include <blazingdb/transport/io/reader_writer.h>
#include <cuda.h>
#include <chrono>
#include "cudf/types.h"
// #include "utils/StringInfo.h"
#include "utils/column_factory.h"
// #include "utils/gpu_functions.h"

#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <nvstrings/NVCategory.h>
#include <boost/crc.hpp>
#include <memory>
#include <numeric>
#include <thread>
#include "rmm/rmm.h"
#include <rmm/device_buffer.hpp>

namespace blazingdb {
namespace transport {

namespace experimental {
// Alias
using SimpleServer = blazingdb::transport::experimental::Server;
using GPUMessage = blazingdb::transport::experimental::GPUMessage;

constexpr uint32_t context_token = 3465;

using GpuFunctions = blazingdb::test::GpuFunctions;
using StringsInfo = blazingdb::test::StringsInfo;

typedef int nv_category_index_type;

struct GPUComponentReceivedMessage : public GPUReceivedMessage {
	GPUComponentReceivedMessage(uint32_t contextToken, const Node &sender_node,
		std::uint64_t total_row_size,
	const std::vector<gdf_column *> &samples)
	: GPUReceivedMessage(GPUComponentReceivedMessage::MessageID(), contextToken, sender_node),
	samples{samples} {
		this->metadata().total_row_size = total_row_size;
	}
	DefineClassName(GPUComponentMessage);

	std::vector<gdf_column *> samples;

};

class GPUComponentMessage : public GPUMessage {
public:
  GPUComponentMessage(uint32_t contextToken, const Node &sender_node,
                      std::uint64_t total_row_size,
                      const std::vector<gdf_column *> &samples)
      : GPUMessage(GPUComponentMessage::MessageID(), contextToken, sender_node),
        samples{samples} {
    this->metadata().total_row_size = total_row_size;
  }

  DefineClassName(GPUComponentMessage);

  // TODO: clean memory
  auto get_raw_pointers(gdf_column *column) {
    void *category_ptr = column->dtype_info.category;
    if (!category_ptr) {
      category_ptr = NVCategory::create_from_array(nullptr, 0);
    }

    auto size = column->size;
    auto null_count = column->null_count;
    std::string output;
    NVCategory *category = static_cast<NVCategory *>(category_ptr);
    NVStrings *nvStrings_ = category->gather_strings(
        static_cast<nv_category_index_type *>(column->data), category->size(),
        true);
    if (!nvStrings_) {
      throw std::runtime_error("nvStrings_ is null in get_raw_pointers");
    }
    gdf_size_type stringsLength_ = nvStrings_->size();
    auto offsetsLength_ = stringsLength_ + 1;

    int *const lengthPerStrings = new int[stringsLength_];
    nvStrings_->byte_count(lengthPerStrings, false);
    auto stringsSize_ =
        std::accumulate(lengthPerStrings, lengthPerStrings + stringsLength_, 0,
                        [](int accumulator, int currentValue) {
                          return accumulator + std::max(currentValue, 0);
                        });
    gdf_size_type offsetsSize_ = offsetsLength_ * sizeof(int);
    char *stringsPointer_;
    int *offsetsPointer_;
    RMM_ALLOC(reinterpret_cast<void **>(&stringsPointer_),
              stringsSize_ * sizeof(char), 0);
    RMM_ALLOC(reinterpret_cast<void **>(&offsetsPointer_),
              offsetsSize_ * sizeof(int), 0);
    gdf_size_type nullMaskSize_ = 0;
    unsigned char *nullBitmask_ = nullptr;
    if (null_count > 0) {
      nullMaskSize_ =
          blazingdb::test::get_bitmask_size_in_bytes(stringsLength_);
      RMM_ALLOC(reinterpret_cast<void **>(&nullBitmask_),
                nullMaskSize_ * sizeof(unsigned char), 0);
    }
    nvStrings_->create_offsets(stringsPointer_, offsetsPointer_, nullBitmask_,
                               true);
    size_t totalSize_ = stringsSize_ + offsetsSize_ + nullMaskSize_ +
                        4 * sizeof(const std::size_t);
    delete[] lengthPerStrings;
    return std::make_tuple(stringsPointer_, stringsSize_, offsetsPointer_,
                           offsetsSize_, nullBitmask_, nullMaskSize_);
  }

  virtual raw_buffer GetRawColumns() override {
    std::vector<int> buffer_sizes;
    std::vector<const char *> raw_buffers;
    std::vector<ColumnTransport> column_offset;
    std::vector<std::unique_ptr<rmm::device_buffer>> temp_scope_holder;

    // serializeToBinary(samples);

    for (int i = 0; i < samples.size(); ++i) {
      auto *column = samples[i];
      ColumnTransport col_transport =
          ColumnTransport{ColumnTransport::MetaData{
                              .dtype = column->dtype,
                              .size = column->size,
                              .null_count = column->null_count,
                              .col_name = {},
                          },
                          .data = -1,
                          .valid = -1,
                          .strings_data = -1,
                          .strings_offsets = -1,
                          .strings_nullmask = -1};
      strcpy(col_transport.metadata.col_name, column->col_name);
      std::cout << "column:" << i << "|" << col_transport.metadata.dtype << "|"
                << col_transport.metadata.size << "|"
                << col_transport.metadata.null_count << "|"
                << col_transport.metadata.col_name << std::endl;
      if (blazingdb::test::GpuFunctions::isGdfString(column)) {
        const char *stringsPointer;
        gdf_size_type stringsSize;
        int *offsetsPointer;
        gdf_size_type offsetsSize;
        const unsigned char *nullBitmask;
        gdf_size_type nullMaskSize;
        std::tie(stringsPointer, stringsSize, offsetsPointer, offsetsSize,
                 nullBitmask, nullMaskSize) = get_raw_pointers(column);

        col_transport.strings_data = raw_buffers.size();
        buffer_sizes.push_back(stringsSize);
        raw_buffers.push_back(stringsPointer);

        col_transport.strings_offsets = raw_buffers.size();
        buffer_sizes.push_back(offsetsSize);
        raw_buffers.push_back((const char *)offsetsPointer);

        if (nullBitmask != nullptr) {
          col_transport.strings_nullmask = raw_buffers.size();
          buffer_sizes.push_back(nullMaskSize);
          raw_buffers.push_back((const char *)nullBitmask);
        }
      } else if (column->valid and column->null_count > 0) {
        // case: valid
        col_transport.data = raw_buffers.size();
        buffer_sizes.push_back(
            blazingdb::test::GpuFunctions::getDataCapacity(column));
        raw_buffers.push_back((char *)column->data);
        col_transport.valid = raw_buffers.size();
        buffer_sizes.push_back(
            blazingdb::test::GpuFunctions::getValidCapacity(column));
        raw_buffers.push_back((char *)column->valid);
      } else {
        // case: data
        col_transport.data = raw_buffers.size();
        buffer_sizes.push_back(
            blazingdb::test::GpuFunctions::getDataCapacity(column));
        raw_buffers.push_back((char *)column->data);
      }
      column_offset.push_back(col_transport);
    }
    return std::make_tuple(buffer_sizes, raw_buffers, column_offset, std::move(temp_scope_holder));
  }

  static std::shared_ptr<GPUReceivedMessage> MakeFrom(
      const Message::MetaData &message_metadata,
      const Address::MetaData &address_metadata,
      const std::vector<ColumnTransport> &columns_offsets,
      const std::vector<rmm::device_buffer> &raw_buffers) {  // gpu pointer
    Node node(
        Address::TCP(address_metadata.ip, address_metadata.comunication_port,
                     address_metadata.protocol_port));
    auto num_columns = columns_offsets.size();
    std::vector<gdf_column *> received_samples(num_columns);
    assert(raw_buffers.size() > 0);
    for (size_t i = 0; i < num_columns; ++i) {
      received_samples[i] = new gdf_column;
      auto data_offset = columns_offsets[i].data;
      auto string_offset = columns_offsets[i].strings_data;
      std::cout << "column:" << i << "|" << columns_offsets[i].metadata.dtype
                << "|" << columns_offsets[i].metadata.size << "|"
                << columns_offsets[i].metadata.null_count << "|"
                << columns_offsets[i].metadata.col_name << std::endl;

      if (string_offset != -1) {
        char *stringsPointer =
            (char *)raw_buffers[columns_offsets[i].strings_data].data();
        int *offsetsPointer =
            (int *)raw_buffers[columns_offsets[i].strings_offsets].data();
        unsigned char *nullMaskPointer = nullptr;
        if (columns_offsets[i].strings_nullmask != -1) {
          nullMaskPointer =
              (unsigned char *)raw_buffers[columns_offsets[i].strings_nullmask].data();
        }

        auto nvcategory_ptr = NVCategory::create_from_offsets(
            reinterpret_cast<const char *>(stringsPointer),
            columns_offsets[i].metadata.size,
            reinterpret_cast<const int *>(offsetsPointer),
            reinterpret_cast<const unsigned char *>(nullMaskPointer),
            columns_offsets[i].metadata.null_count, true);

        received_samples[i] = blazingdb::test::create_nv_category_gdf_column(
            nvcategory_ptr, columns_offsets[i].metadata.size,
            (char *)columns_offsets[i].metadata.col_name);
      } else {
        gdf_valid_type *valid_ptr = nullptr;
        if (columns_offsets[i].valid != -1) {
          // this is a valid
          auto valid_offset = columns_offsets[i].valid;
          valid_ptr = (gdf_valid_type *)raw_buffers[valid_offset].data();
        }
        gdf_error err = gdf_column_view_augmented(
            received_samples[i], (void *)raw_buffers[data_offset].data(), valid_ptr,
            (gdf_size_type)columns_offsets[i].metadata.size,
            (gdf_dtype)columns_offsets[i].metadata.dtype,
            (gdf_size_type)columns_offsets[i].metadata.null_count,
            gdf_dtype_extra_info{
                .time_unit = gdf_time_unit(0), 
                .category = nullptr},
            (char *)columns_offsets[i].metadata.col_name);
      }
    }


    // serializeToBinary(received_samples);
    std::string expected_checksum = "41081C4C";
    for (gdf_column *column : received_samples) {
        blazingdb::test::print_gdf_column(column);
    }
    return std::make_shared<GPUComponentReceivedMessage>(
        message_metadata.contextToken, node, message_metadata.total_row_size,
        received_samples);
  }

  std::vector<gdf_column *> samples;
};

std::shared_ptr<GPUMessage> CreateSampleToNodeMaster(
    uint32_t context_token, const Node &sender_node) {
  cudf::table table = blazingdb::test::build_table();
  std::vector<gdf_column *> samples(table.num_columns());
  std::copy(table.begin(), table.end(), samples.begin());

  for (auto &col : samples) {
    blazingdb::test::print_gdf_column(col);
  }
  std::uint64_t total_row_size = samples[0]->size;
  auto gpu_message = std::make_shared<GPUComponentMessage>(context_token, sender_node, total_row_size, samples);
  std::vector<const char *> buffers;
  std::vector<int> buffer_sizes;
  std::vector<ColumnTransport> column_offsets;
  std::vector<std::unique_ptr<rmm::device_buffer>> temp_scope_holder;
  std::tie(buffer_sizes, buffers, column_offsets, temp_scope_holder) =
      gpu_message->GetRawColumns();
  for (auto &info : column_offsets) {
    std::cout << "\t " << info.strings_data << "|" << info.strings_offsets
              << info.data << "\n";
  }
  std::cout << "## buffer_sizes\n";
  for (int index = 0; index < buffer_sizes.size(); index++) {
    auto gpu_buffer_sz = buffer_sizes[index];
    auto gpu_buffer = buffers[index];
    std::vector<char> result(gpu_buffer_sz);
    cudaMemcpy(result.data(), gpu_buffer, gpu_buffer_sz,
               cudaMemcpyDeviceToHost);
    boost::crc_32_type crc_result;
    crc_result.process_bytes((char *)result.data(), result.size());
    std::cout << "\tchecksum_col(" << index << "):" << std::hex
              << std::uppercase << crc_result.checksum() << std::endl;
  }
  // auto gpu_message_copy = std::dynamic_pointer_cast<GPUComponentMessage>(
  //     GPUComponentMessage::MakeFrom(
  //         gpu_message->metadata(),
  //         gpu_message->getSenderNode().address().metadata(), column_offsets,
  //         buffers));
  // std::cout << "## original_message\n";
  // serializeToBinary(gpu_message->samples);
  // std::cout << "## copy_message\n";
  // serializeToBinary(gpu_message_copy->samples);
  return gpu_message;
}

class RalServer {
public:
  static void start(unsigned short port = 8000) {
    port_ = port;
    getInstance();
  }

  static void close() { getInstance().comm_server->Close(); }

  static RalServer &getInstance() {
    static RalServer server;
    return server;
  }

private:
  RalServer() {
    comm_server = SimpleServer::TCP(port_);
    setEndPoints();
    comm_server->Run();
  }

public:
  ~RalServer() {}

public:
  void registerContext(const uint32_t &context_token) {
    comm_server->registerContext(context_token);
  }

public:
  std::shared_ptr<Message> getMessage(const uint32_t &token_value,
                                      const std::string &messageToken) {
    return comm_server->getMessage(token_value, messageToken);
  }

private:
  RalServer(RalServer &&) = delete;

  RalServer(const RalServer &) = delete;

  RalServer &operator=(RalServer &&) = delete;

  RalServer &operator=(const RalServer &) = delete;

private:
  // instanced at Construction time
  void setEndPoints() {
    // message SampleToNodeMasterMessage
    {
      const std::string endpoint = GPUComponentMessage::MessageID();
      comm_server->registerEndPoint(endpoint);
      comm_server->registerMessageForEndPoint(GPUComponentMessage::MakeFrom, endpoint);
    }
  }

private:
  std::shared_ptr<SimpleServer> comm_server;

private:
  static unsigned short port_;
};
unsigned short RalServer::port_ = 0;

class RalClient {
public:
public:
  static Status send(const Node &node, GPUMessage &message) {
    auto client = blazingdb::transport::experimental::ClientTCP::Make(
        node.address().metadata().ip,
        node.address().metadata().comunication_port);
    std::cout << "send message\n";
    return client->Send(message);
  }
  static Status sendNodeData(const std::string &orchestratorIp,
                             int16_t orchestratorPort, GPUMessage &message) {
    auto client =
        blazingdb::transport::experimental::ClientTCP::Make(orchestratorIp, orchestratorPort);
    return client->Send(message);
  }
};
// TODO get GPU_MEMORY_SIZE
auto GPU_MEMORY_SIZE = 100;

static void ExecMaster() {
  cuInit(0);
  ASSERT_EQ(rmmInitialize(nullptr), RMM_SUCCESS);
  // Run server
  RalServer::start(8000);

  auto sizeBuffer = GPU_MEMORY_SIZE / 4;
  blazingdb::transport::experimental::io::setPinnedBufferProvider(sizeBuffer, 1);
  RalServer::getInstance().registerContext(context_token);
  std::thread([]() {
    // while(true){
      auto message = RalServer::getInstance().getMessage(context_token, GPUComponentMessage::MessageID());
      auto concreteMessage = std::static_pointer_cast<GPUComponentMessage>(message);
      std::cout << "***Message begin\n";
      // for (gdf_column *column : concreteMessage->samples) {
      //   blazingdb::test::print_gdf_column(column);
      // }
      std::cout << "***Message end\n";
    // }
    RalServer::getInstance().close();
  }).join();
}

static void ExecWorker() {
  cuInit(0);
  ASSERT_EQ(rmmInitialize(nullptr), RMM_SUCCESS);
  // todo get GPU_MEMORY_SIZE
  auto sizeBuffer = GPU_MEMORY_SIZE / 4;
  auto nthread = 4;
  blazingdb::transport::experimental::io::setPinnedBufferProvider(sizeBuffer, nthread);
  // This lines are not necessary!!
  //  RalServer::start(8001);
  //  RalServer::getInstance().registerContext(context_token);

  auto sender_node =
      std::make_shared<Node>(Address::TCP("127.0.0.1", 8001, 1234));
  auto server_node =
      std::make_shared<Node>(Address::TCP("127.0.0.1", 8000, 1234));

  auto message = CreateSampleToNodeMaster(context_token, *sender_node);
  RalClient::send(*server_node, *message);
}

// TODO: move common code of TCP client and server to blazingdb::network in
// order to be shared by manager and transport
// TODO: check when the ip, port is busy, return exception!
// TODO: check when the message is not registered, or the wrong message is
// registered
TEST(SendSamplesTest, MasterAndWorker) {
 if (fork() > 0) {
   ExecMaster();
 } else {
   ExecWorker();
 }
}

// TEST(SendSamplesTest, Master) { ExecMaster(); }

// TEST(SendSamplesTest, Worker) { ExecWorker(); }

}  // namespace experimental
}  // namespace transport
}  // namespace blazingdb
