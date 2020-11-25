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
		std::shared_ptr<ral::cache::CacheMachine> input_cache,
		std::map<std::string, node> node_address_map,
		int num_threads,
		ucp_context_h context,
		ucp_worker_h origin_node,
		uint16_t ral_id,
		comm::blazing_protocol protocol){
	
	if(instance == NULL) {
		message_sender::instance = new message_sender(
				output_cache,input_cache,node_address_map,num_threads,context,origin_node,ral_id,protocol);
	}
}

message_sender::message_sender(std::shared_ptr<ral::cache::CacheMachine> output_cache,
		std::shared_ptr<ral::cache::CacheMachine> input_cache,
		const std::map<std::string, node> & node_address_map,
		int num_threads,
		ucp_context_h context,
		ucp_worker_h origin,
		uint16_t ral_id,
		comm::blazing_protocol protocol)
		: ral_id{ral_id}, origin{origin}, output_cache{output_cache}, input_cache{input_cache}, node_address_map{node_address_map}, pool{num_threads}, protocol{protocol}
{

	request_size = 0;
	if (protocol == blazing_protocol::ucx)	{
		ucp_context_attr_t attr;
		attr.field_mask = UCP_ATTR_FIELD_REQUEST_SIZE;
		ucs_status_t status = ucp_context_query(context, &attr);
		if (status != UCS_OK)	{
			throw std::runtime_error("Error calling ucp_context_query");
		}

		request_size = attr.request_size;
	}else if (protocol == blazing_protocol::tcp){

	}else{
		std::cout<<"Wrong protocol"<<std::endl;
	}
}

} // namespace comm
