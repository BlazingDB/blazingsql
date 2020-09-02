#include "messageReceiver.hpp"
#include "protocols.hpp"

namespace comm {

 node message_receiver::get_sender_node(){
    return ucp_nodes_info::getInstance().get_node(_metadata.get_values()[ral::cache::SENDER_WORKER_ID_METADATA_LABEL]);
  }


message_receiver::message_receiver(const std::vector<char> & buffer)
    
  {


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

     if (_raw_buffers.size() == 0) {
       finish();
     }
  }

} // namespace comm
