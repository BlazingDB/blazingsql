#include "messageReceiver.hpp"
#include "protocols.hpp"

namespace comm {

message_receiver::message_receiver(const std::map<std::string, comm::node>& nodes, const std::vector<char> & buffer)
{
  _nodes_info_map = nodes;
  auto metadata_and_transports = detail::get_metadata_and_transports_and_buffer_sizes_from_bytes(buffer);
  _metadata = std::get<0>(metadata_and_transports);
  _column_transports = std::get<1>(metadata_and_transports);
  _buffer_sizes = std::get<2>(metadata_and_transports);
  int32_t ctx_token = std::stoi(_metadata.get_values()[ral::cache::QUERY_ID_METADATA_LABEL]);
  auto graph = graphs_info::getInstance().get_graph(ctx_token);
  size_t kernel_id = std::stoull(_metadata.get_values()[ral::cache::KERNEL_ID_METADATA_LABEL]);
  std::string cache_id = _metadata.get_values()[ral::cache::CACHE_ID_METADATA_LABEL];
  _output_cache = _metadata.get_values()[ral::cache::ADD_TO_SPECIFIC_CACHE_METADATA_LABEL] == "true" ?
                      graph->get_kernel_output_cache(kernel_id, cache_id) : graph->get_input_message_cache();

_raw_buffers.resize(_buffer_sizes.size());
}

size_t message_receiver::buffer_size(u_int16_t index){
  return _buffer_sizes[index];
}

void message_receiver::allocate_buffer(uint16_t index, cudaStream_t stream){
  if (index >= _raw_buffers.size()) {
    throw std::runtime_error("Invalid access to raw buffer");
  }
  _raw_buffers[index].resize(_buffer_sizes[index],stream);
}

node message_receiver::get_sender_node(){
  return _nodes_info_map.at(_metadata.get_values()[ral::cache::SENDER_WORKER_ID_METADATA_LABEL]);
}

size_t message_receiver::num_buffers(){
  return _buffer_sizes.size();
}

void message_receiver::confirm_transmission(){
  ++_buffer_counter;
  if (_buffer_counter == _raw_buffers.size()) {
    finish();
  }
}

void * message_receiver::get_buffer(uint16_t index){
  return _raw_buffers[index].data();
}

bool message_receiver::is_finished(){
  return (_buffer_counter == _raw_buffers.size());
}

void message_receiver::finish(cudaStream_t stream) {
  std::unique_ptr<ral::frame::BlazingTable> table = deserialize_from_gpu_raw_buffers(_column_transports, _raw_buffers,stream);
  if ( _metadata.get_values()[ral::cache::ADD_TO_SPECIFIC_CACHE_METADATA_LABEL] == "true"){
    _output_cache->addToCache(std::move(table),  _metadata.get_values()[ral::cache::MESSAGE_ID], true);
  } else {
    _output_cache->addCacheData(
            std::make_unique<ral::cache::GPUCacheDataMetaData>(std::move(table), _metadata), _metadata.get_values()[ral::cache::MESSAGE_ID], true);
  }
}

} // namespace comm
