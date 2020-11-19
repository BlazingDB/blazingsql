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
		int ral_id,
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
		int ral_id,
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

void message_sender::run_polling() {
	if (!polling_started){
		polling_started = true;

		auto thread = std::thread([this]{
		// cudaSetDevice(0);

		while(true) {
			std::vector<std::unique_ptr<ral::cache::CacheData> > cache_datas = output_cache->pull_all_cache_data();
			for(auto & cache_data : cache_datas){
				pool.push([cache_data{std::move(cache_data)},
						node_address_map = node_address_map,
						output_cache = output_cache,
							protocol=this->protocol,
							this](int thread_id) {

					auto * gpu_cache_data = static_cast<ral::cache::GPUCacheDataMetaData *>(cache_data.get());
					auto data_and_metadata = gpu_cache_data->decacheWithMetaData();
					auto & metadata = data_and_metadata.second;
					auto & table = data_and_metadata.first;

					std::vector<std::size_t> buffer_sizes;
					std::vector<const char *> raw_buffers;
					std::vector<blazingdb::transport::ColumnTransport> column_transports;
					std::vector<std::unique_ptr<rmm::device_buffer>> temp_scope_holder;

					std::tie(buffer_sizes, raw_buffers, column_transports, temp_scope_holder) =
						serialize_gpu_message_to_gpu_containers(table->toBlazingTableView());

					try {
						// tcp / ucp
						auto metadata_map = metadata.get_values();

						std::vector<node> destinations;

						auto worker_ids = StringUtil::split(metadata_map.at(ral::cache::WORKER_IDS_METADATA_LABEL), ",");
						for(auto worker_id : worker_ids) {

							if(node_address_map.find(worker_id) == node_address_map.end()) {
								throw std::runtime_error("Worker id not found!" + worker_id);
							}
							destinations.push_back(node_address_map.at(worker_id));
						}

						std::shared_ptr<buffer_transport> transport;
						if(blazing_protocol::ucx == protocol){

							transport = std::make_shared<ucx_buffer_transport>(
								request_size, origin, destinations, metadata,
								buffer_sizes, column_transports,ral_id);
						}else if (blazing_protocol::tcp == protocol){

							transport = std::make_shared<tcp_buffer_transport>(
							destinations,
							metadata,
							buffer_sizes,
							column_transports,
							ral_id,
							&this->pool
							);
						}
						else{
							throw std::runtime_error("Unknown protocol");
						}

						transport->send_begin_transmission();
						transport->wait_for_begin_transmission();
						for(size_t i = 0; i < raw_buffers.size(); i++) {
							transport->send(raw_buffers[i], buffer_sizes[i]);
						}
						transport->wait_until_complete();  // ensures that the message has been sent before returning the thread to the pool
					} catch(const std::exception&) {
						throw;
					}
			});
		}
		output_cache->wait_for_next();
		}
	});
	thread.detach();
	}
}

} // namespace comm
