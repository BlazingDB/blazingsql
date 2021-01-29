#include "messageReceiver.hpp"
#include "protocols.hpp"
#include <spdlog/spdlog.h>


namespace comm {
using namespace fmt::literals;
message_receiver::message_receiver(const std::map<std::string, comm::node>& nodes, const std::vector<char> & buffer) 
: _buffer_counter{0}
{

  try {
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
  //_metadata.print();
  _raw_buffers.resize(_buffer_sizes.size());
    std::shared_ptr<spdlog::logger> comms_logger;
    comms_logger = spdlog::get("input_comms");
    auto destinations = _metadata.get_values()[ral::cache::WORKER_IDS_METADATA_LABEL];

    comms_logger->info("{ral_id}|{query_id}|{kernel_id}|{dest_ral_id}|{dest_ral_count}|{dest_cache_id}|{message_id}|{phase}",
    "ral_id"_a=_metadata.get_values()[ral::cache::RAL_ID_METADATA_LABEL],
    "query_id"_a=_metadata.get_values()[ral::cache::QUERY_ID_METADATA_LABEL],
    "kernel_id"_a=_metadata.get_values()[ral::cache::KERNEL_ID_METADATA_LABEL],
    "dest_ral_id"_a=destinations, //false
    "dest_ral_count"_a=std::count(destinations.begin(), destinations.end(), ',') + 1,
    "dest_cache_id"_a=_metadata.get_values()[ral::cache::CACHE_ID_METADATA_LABEL],
    "message_id"_a=_metadata.get_values()[ral::cache::MESSAGE_ID],
    "phase"_a="begin");
  } catch(const std::exception & e) {
    auto logger = spdlog::get("batch_logger");
    if (logger){
      logger->error("|||{info}|||||",
          "info"_a="ERROR in message_receiver::message_receiver. What: {}"_format(e.what()));
    }
    throw;
  }
}

size_t message_receiver::buffer_size(u_int16_t index){
  return _buffer_sizes[index];
}

void message_receiver::allocate_buffer(uint16_t index, cudaStream_t stream){
  if (index >= _raw_buffers.size()) {
    throw std::runtime_error("Invalid access to raw buffer");
  }
  _raw_buffers[index].resize(_buffer_sizes[index]);//,stream);
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
    //this is ugly. We are currently doing this because
    //the UCX apis expect a void * instead of a const void * for
    //buffers it will fill but that it wont allocate
    return &_raw_buffers[index][0];
}

bool message_receiver::is_finished(){
  return (_buffer_counter == _raw_buffers.size());
}

void message_receiver::finish(cudaStream_t stream) {

  std::lock_guard<std::mutex> lock(_finish_mutex);
  if(!_finished_called){
    _finished_called = true;
    std::shared_ptr<spdlog::logger> comms_logger;
    comms_logger = spdlog::get("input_comms");
    auto destinations = _metadata.get_values()[ral::cache::WORKER_IDS_METADATA_LABEL];

    comms_logger->info("{ral_id}|{query_id}|{kernel_id}|{dest_ral_id}|{dest_ral_count}|{dest_cache_id}|{message_id}|{phase}",
                        "ral_id"_a=_metadata.get_values()[ral::cache::RAL_ID_METADATA_LABEL],
                        "query_id"_a=_metadata.get_values()[ral::cache::QUERY_ID_METADATA_LABEL],
                        "kernel_id"_a=_metadata.get_values()[ral::cache::KERNEL_ID_METADATA_LABEL],
                        "dest_ral_id"_a=destinations, //false
                        "dest_ral_count"_a=std::count(destinations.begin(), destinations.end(), ',') + 1,
                        "dest_cache_id"_a=_metadata.get_values()[ral::cache::CACHE_ID_METADATA_LABEL],
                        "message_id"_a=_metadata.get_values()[ral::cache::MESSAGE_ID],
                        "phase"_a="end");
    
    // TODO:FELIPE  here we need change the code to go from _raw_buffers to:
    std::vector<ral::memory::blazing_chunked_buffer> _buffers;
    std::vector<std::unique_ptr<ral::memory::blazing_allocation_chunk>> _allocations;

    std::unique_ptr<ral::cache::CacheData> table = 
        std::make_unique<ral::cache::CPUCacheData>(_column_transports,std::move(_buffers),std::move(_allocations), _metadata);
    _output_cache->addCacheData(
                std::move(table), _metadata.get_values()[ral::cache::MESSAGE_ID], true);  
  }

}

} // namespace comm
