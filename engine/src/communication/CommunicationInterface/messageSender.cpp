#include "messageSender.hpp"

namespace comm {

message_sender * message_sender::instance = nullptr;


message_sender * message_sender::get_instance() {
	if(instance == NULL) {
        throw std::exception();
	}
	return instance;
}

void message_sender::initialize_instance(std::shared_ptr<ral::cache::CacheMachine> output_cache,
		std::map<std::string, node> node_address_map,
		int num_threads,
		ucp_worker_h origin_node,
		int ral_id){
        message_sender::instance = new message_sender(
            output_cache,node_address_map,num_threads,origin_node,ral_id);
}

} // namespace comm
