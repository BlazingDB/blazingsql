#pragma once

#include <vector>
#include <map>
#include <memory>
#include <rmm/device_buffer.hpp>
#include <blazingdb/transport/ColumnTransport.h>

#include "serializer.hpp"
#include "execution_graph/logic_controllers/CacheMachine.h"

namespace comm {

  /**
  * @brief A Class used for the reconstruction of a BlazingTable from
  * metadata and column data
  */
class message_receiver {
using ColumnTransport = blazingdb::transport::ColumnTransport;

public:

  /**
  * @brief Constructs a message_receiver.
  *
  * This is a place for a message to receive chunks. It calls the deserializer after the complete
  * message has been assembled
  *
  * @param column_transports This is metadata about how a column will be reconstructed used by the deserialzer
  * @param metadata This is information about how the message was routed and payloads that are used in
  *                 execution, planning, or physical optimizations. E.G. num rows in table, num partitions to be processed
  * @param output_cache The destination for the message being received. It is either a specific cache inbetween
  *                     two kernels or it is intended for the general input cache using a mesage_id
  * @param include_metadata Indicates if we should add the metadata along with the payload to the cache or if we should not.
  */
  message_receiver(const std::vector<ColumnTransport> & column_transports,
                  const ral::cache::MetadataDictionary & metadata,
                  std::shared_ptr<ral::cache::CacheMachine> output_cache) :
    _column_transports{column_transports},
    _output_cache{output_cache},
    _metadata{metadata}
  {
    _raw_buffers.resize(column_transports.size());

     if (_raw_buffers.size() == 0) {
       finish();
     }
  }

  void set_buffer_size(uint16_t index,size_t size){
    if (index >= _raw_buffers.size()) {
      throw std::runtime_error("Invalid access to raw buffer");
    }
    _raw_buffers[index].resize(size);

  }

  void confirm_transmission(){
    ++_buffer_counter;
    if (_buffer_counter == _raw_buffers.size()) {
      finish();
    }
  }

  void * get_buffer(uint16_t index){
    return _raw_buffers[index].data();
  }
  /**
  * @brief Adds a device_buffer to a specific position used for the reconstruction
  * of a BlazingTable
  *
  * @param buffer A device_buffer containing part of the data of a BlazingTable
  * @param index The position of the buffer
  */
  void add_buffer(rmm::device_buffer buffer, uint16_t index) {

    _raw_buffers[index] = std::move(buffer);

    ++_buffer_counter;
    if (_buffer_counter == _raw_buffers.size()) {
      finish();
    }
  }

  bool is_finished(){
    return finished;
  }
private:
  void finish() {
    std::unique_ptr<ral::frame::BlazingTable> table = deserialize_from_gpu_raw_buffers(_column_transports, _raw_buffers);
    if(_include_metadata){
      _output_cache->addCacheData(
            std::make_unique<ral::cache::GPUCacheDataMetaData>(std::move(table), _metadata), _metadata.get_values()[ral::cache::MESSAGE_ID], true);
    } else {
      _output_cache->addToCache(std::move(table), "", true);
    }
    finished = true;
  }

  std::vector<ColumnTransport> _column_transports;
  std::shared_ptr<ral::cache::CacheMachine> _output_cache;
  ral::cache::MetadataDictionary _metadata;
  bool _include_metadata;

  std::vector<rmm::device_buffer> _raw_buffers;
  int _buffer_counter = 0;
  bool finished = false;
};

} // namespace comm
