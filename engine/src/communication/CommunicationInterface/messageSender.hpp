#pragma once

#include <blazingdb/transport/ColumnTransport.h>
#include <memory>
#include <rmm/device_buffer.hpp>
#include <utility>
#include <Util/StringUtil.h>

#include "blazingdb/concurrency/BlazingThread.h"
#include "node.hpp"
#include "serializer.hpp"
#include "execution_graph/logic_controllers/CacheMachine.h"
#include "utilities/ctpl_stl.h"
#include "protocols.hpp"

namespace comm {

/**
 * @brief A Class that can be used to poll messages and then send them off.
 */
class message_sender {
public:

	static message_sender * get_instance();
	/**
	 * @brief Constructs a message_sender
	 *
	 * @param output_cache The cache machine from where to obtain the data to send
	 * @param input_cache The cache machine there the node receives data (not acutally used by this class, but this class maintains its scope)
	 * @param node_address_map A map from node id to Node
	 * @param num_threads Number of threads the message_sender will use to send data concurrently
	 * @param context The ucp_context_h
	 * @param origin The ucp_worker_h
	 * @param ral_id The ral_id
	 * @param protocol The comm::blazing_protocol 
	 */
	message_sender(std::shared_ptr<ral::cache::CacheMachine> output_cache,
		std::shared_ptr<ral::cache::CacheMachine> input_cache,
		const std::map<std::string, node> & node_address_map,
		int num_threads,
		ucp_context_h context,
		ucp_worker_h origin,
		int ral_id,
		comm::blazing_protocol protocol);

	static void initialize_instance(std::shared_ptr<ral::cache::CacheMachine> output_cache,
		std::shared_ptr<ral::cache::CacheMachine> input_cache,
		std::map<std::string, node> node_address_map,
		int num_threads,
		ucp_context_h context,
		ucp_worker_h origin_node,
		int ral_id,
		comm::blazing_protocol protocol);

	std::shared_ptr<ral::cache::CacheMachine> get_output_cache(){
		return output_cache;
	}
	std::shared_ptr<ral::cache::CacheMachine> get_input_cache(){
		return input_cache;
	}

	/**
	 * @brief A polling function that listens on a cache for data and send it off via some protocol
	 */
	void run_polling() {
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
private:
	static message_sender * instance;


	ctpl::thread_pool<BlazingThread> pool;
	std::shared_ptr<ral::cache::CacheMachine> output_cache;
	std::shared_ptr<ral::cache::CacheMachine> input_cache;
	std::map<std::string, node> node_address_map;
	blazing_protocol protocol;
	ucp_worker_h origin;
	size_t request_size;
	int ral_id;
	bool polling_started{false};
};

}  // namespace comm
