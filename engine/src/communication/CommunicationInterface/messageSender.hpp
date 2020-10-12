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
	 * @param node_address_map A map from node id to Node
	 * @param num_threads Number of threads the message_sender will use to send data concurrently
	 */
	message_sender(std::shared_ptr<ral::cache::CacheMachine> output_cache,
		const std::map<std::string, node> & node_address_map,
		int num_threads,
		ucp_context_h context,
		ucp_worker_h origin,
		int ral_id,
		comm::blazing_protocol protocol);

	static void initialize_instance(std::shared_ptr<ral::cache::CacheMachine> output_cache,
		std::map<std::string, node> node_address_map,
		int num_threads,
		ucp_context_h context,
		ucp_worker_h origin_node,
		int ral_id,
		comm::blazing_protocol protocol);
	/**
	 * @brief A polling function that listens on a cache for data and send it off via some protocol
	 */
	void run_polling() {
		std::cout<<"WWWWWWWWWWWWWWW   message_sender run_polling start"<<std::endl;
		if (!polling_started){
			std::cout<<"WWWWWWWWWWWWWWW   message_sender run_polling start actually going to start"<<std::endl;
			polling_started = true;
		
			auto thread = std::thread([this]{
				cudaSetDevice(0);

				while(true) {
					std::cout<<"goignt to pull"<<std::endl;
					std::unique_ptr<ral::cache::CacheData> cache_data = output_cache->pullCacheData();
					std::cout<<"pulled "<<cache_data.get()<<std::endl;
					auto * gpu_cache_data = static_cast<ral::cache::GPUCacheDataMetaData *>(cache_data.get());
					auto data_and_metadata = gpu_cache_data->decacheWithMetaData();

					pool.push([table{move(data_and_metadata.first)},
								metadata{data_and_metadata.second},
								node_address_map = node_address_map,
								output_cache = output_cache,
									protocol=this->protocol,
									this](int thread_id) {
						std::cout<<"in the pool!"<<std::endl;
						std::vector<std::size_t> buffer_sizes;
						std::vector<const char *> raw_buffers;
						std::vector<blazingdb::transport::ColumnTransport> column_transports;
						std::vector<std::unique_ptr<rmm::device_buffer>> temp_scope_holder;
						std::cout<<"starting serialize"<<std::endl;
						std::tie(buffer_sizes, raw_buffers, column_transports, temp_scope_holder) =
							serialize_gpu_message_to_gpu_containers(table->toBlazingTableView());
						std::cout<<"finished serialized"<<std::endl;
						try {
							// tcp / ucp
							auto metadata_map = metadata.get_values();

							std::vector<node> destinations;
							std::cout<<"1"<<std::endl;
							auto worker_ids = StringUtil::split(metadata_map.at(ral::cache::WORKER_IDS_METADATA_LABEL), ",");
							for(auto worker_id : worker_ids) {
								std::cout<<"im here"<<std::endl;
								if(node_address_map.find(worker_id) == node_address_map.end()) {
									std::cout<<"Worker id not found!"<<worker_id<<std::endl;
									throw std::exception();	 // TODO: make a real exception here
								}
								destinations.push_back(node_address_map.at(worker_id));
							}
							std::cout<<"2"<<std::endl;
							std::shared_ptr<buffer_transport> transport;
							if(blazing_protocol::ucx == protocol){
								std::cout<<"in ucx"<<std::endl;
								transport = std::make_shared<ucx_buffer_transport>(
									request_size, origin, destinations, metadata,
									buffer_sizes, column_transports,ral_id);
							}else if (blazing_protocol::tcp == protocol){
								std::cout<<"in tcp"<<std::endl;
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
								std::cout<<"how!?"<<std::endl;
								throw std::exception();
							}
							std::cout<<"sending begin transmission"<<std::endl;
							transport->send_begin_transmission();
							std::cout<<"waiting for send"<<std::endl;
							transport->wait_for_begin_transmission();
							std::cout<<"sent begin"<<std::endl;
							for(size_t i = 0; i < raw_buffers.size(); i++) {
								std::cout<<"sending "<<i<<std::endl;
								transport->send(raw_buffers[i], buffer_sizes[i]);
							std::cout<<"sent "<<i<<std::endl;
							}
							std::cout<<"waiting complete"<<std::endl;
							transport->wait_until_complete();  // ensures that the message has been sent before returning the thread to the pool
							std::cout<<"complete"<<std::endl;
						} catch(const std::exception&) {
							throw;
						}
					});
				}
			});
			thread.detach();
		}
	}
private:
	static message_sender * instance;


	ctpl::thread_pool<BlazingThread> pool;
	std::shared_ptr<ral::cache::CacheMachine> output_cache;
	std::map<std::string, node> node_address_map;
	blazing_protocol protocol;
	ucp_worker_h origin;
	size_t request_size;
	int ral_id;
	bool polling_started{false};
};

}  // namespace comm
