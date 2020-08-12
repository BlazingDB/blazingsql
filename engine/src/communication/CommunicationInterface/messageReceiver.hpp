#pragma once

#include <vector>
#include <map>
#include <memory>
#include <rmm/device_buffer.hpp>
#include <blazingdb/transport/ColumnTransport.h>

#include "serializer.hpp"
#include "execution_graph/logic_controllers/CacheMachine.h"

namespace comm {

class message_receiver {
using ColumnTransport = blazingdb::transport::ColumnTransport;

public:

  /**
  * Constructor for the message receiver.
  * This is a place for a message to receive chunks. It calls the deserializer after the complete
  * message has been assembled
  * @param column_transports This is metadata about how a column will be reconstructed used by the deserialzer
  * @param buffer_id_to_positions each buffer will have an id which lets us know what position it maps to in the
  *                               list of buffers and the deserializer it will use.
  * @param output_cache The destination for the message being received. It is either a specific cache inbetween
  *                     two kernels or it is intended for the general input cache using a mesage_id
  * @param metadata This is information about how the message was routed and payloads that are used in
  *                 execution, planning, or physical optimizations. E.G. num rows in table, num partitions to be processed
  * @param include_metadata Indicates if we should add the metadata along with the payload to the cache or if we should not.
  */
  message_receiver(const std::vector<ColumnTransport> & column_transports,
                  const std::map<std::string, size_t> & buffer_id_to_position,
                  const ral::cache::MetadataDictionary & metadata,
                  bool include_metadata,
                  std::shared_ptr<ral::cache::CacheMachine> output_cache) :
    _column_transports{column_transports},
    _buffer_id_to_position{buffer_id_to_position},
    _output_cache{output_cache},
    _metadata{metadata},
    _include_metadata{include_metadata}
  {
    _raw_buffers.resize(column_transports.size());

    // if (_raw_buffers.size() == 0) {
    //   finish();
    // }
  }

  void add_buffer(rmm::device_buffer buffer, std::string id) {
    _raw_buffers[_buffer_id_to_position.at(id)] = std::move(buffer);

    ++_buffer_counter;
    if (_buffer_counter == _raw_buffers.size()) {
      finish();
    }
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
  }

  std::vector<ColumnTransport> _column_transports;
  std::map<std::string,size_t> _buffer_id_to_position;
  std::shared_ptr<ral::cache::CacheMachine> _output_cache;
  ral::cache::MetadataDictionary _metadata;
  bool _include_metadata;

  std::vector<rmm::device_buffer> _raw_buffers;
  int _buffer_counter = 0;
};

} // namespace comm
