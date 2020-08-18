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
		std::map<std::string, node> node_address_map,
		int num_threads)
		: output_cache{output_cache}, pool{num_threads}, protocol{blazing_protocol::ucx} {}
	static void initialize_instance(std::shared_ptr<ral::cache::CacheMachine> output_cache,
		std::map<std::string, node> node_address_map,
		int num_threads);
	/**
	 * @brief A polling function that listens on a cache for data and send it off via some protocol
	 */
	void run_polling() {
		while(true) {
			std::unique_ptr<ral::cache::CacheData> cache_data = output_cache->pullCacheData();
			auto * gpu_cache_data = static_cast<ral::cache::GPUCacheDataMetaData *>(cache_data.get());
			auto data_and_metadata = gpu_cache_data->decacheWithMetaData();

			pool.push([table{move(data_and_metadata.first)},
						  metadata{data_and_metadata.second},
						  node_address_map = node_address_map,
						  output_cache = output_cache,
							protocol=this->protocol](int thread_id) {
				std::vector<std::size_t> buffer_sizes;
				std::vector<const char *> raw_buffers;
				std::vector<blazingdb::transport::ColumnTransport> column_transports;
				std::vector<std::unique_ptr<rmm::device_buffer>> temp_scope_holder;
				std::tie(buffer_sizes, raw_buffers, column_transports, temp_scope_holder) =
					serialize_gpu_message_to_gpu_containers(table->toBlazingTableView());

				try {
					// Initializes the sender with information needed for communicating the function that begins
					// transmission This constructor will make a ucx call iwth all the metadata and wait for it to
					// complete
					// if(/* condition */) {
					// 	/* code */
					// }

					// tcp / ucp

					auto metadata_map = metadata.get_values();

					node origin = node_address_map.at(metadata_map.at(ral::cache::SENDER_WORKER_ID_METADATA_LABEL));

					std::vector<node> destinations;
					for(auto worker_id : StringUtil::split(metadata_map.at(ral::cache::WORKER_IDS_METADATA_LABEL), ",")) {
						if(node_address_map.find(worker_id) == node_address_map.end()) {
							throw std::exception();	 // TODO: make a real exception here
						}
						destinations.push_back(node_address_map.at(worker_id));
					}

					std::shared_ptr<buffer_transport> transport;
					if(blazing_protocol::ucx == protocol){
						transport = std::make_shared<ucx_buffer_transport>(
							origin, destinations, metadata,
							buffer_sizes, column_transports);
					}else{
						throw std::exception();
					}

					transport->send_begin_transmission();
					for(size_t i = 0; i < raw_buffers.size(); i++) {
						transport->send(raw_buffers[i], buffer_sizes[i]);
						// temp_scope_holder[buffer_index] = nullptr;	// TODO: allow the device_vector to go out of
						// scope
					}
					transport->wait_until_complete();  // ensures that the message has been sent before returning the thread
												   // to the pool
				} catch(const std::exception&) {
					throw;
				}
			});
		}
	}
private:
	static message_sender * instance;
	

	ctpl::thread_pool<BlazingThread> pool;
	std::shared_ptr<ral::cache::CacheMachine> output_cache;
	std::map<std::string, node> node_address_map;
	blazing_protocol protocol;
};

}  // namespace comm
