#pragma once

#include <blazingdb/transport/ColumnTransport.h>
#include <memory>
#include <rmm/device_buffer.hpp>
#include <utility>

#include "blazingdb/concurrency/BlazingThread.h"
#include "blazingdb/transport/Node.h"
#include "bufferTransport.hpp"
#include "execution_graph/logic_controllers/CacheMachine.h"
#include "utilities/ctpl_stl.h"

namespace comm {

/**
 * A Class that can be used to poll messages and then send them off.
 * Creating this class serves the purpose of allowing us to specify different combinations of serializers, conversion
 * from buffer to frame combined with different methods for sending and addressing.
 */
template <typename buffer_transport>
class message_sender {
public:
	message_sender(std::shared_ptr<ral::cache::CacheMachine> output_cache,
		std::map<std::string, blazingdb::transport::Node> node_address_map,
		size_t num_threads)
		: output_cache{output_cache}, pool{num_threads} {}

private:
	/**
	 * A polling function that listens on a cache for data to exist and then sends it off via some protocol
	 */
	void run_polling() {
		while(true) {
			auto * gpu_cache_data = static_cast<ral::cache::GPUCacheDataMetaData *>(output_cache->pullCacheData());
			auto data_and_metadata = gpu_cache_data->decacheWithMetaData();

			pool.push([table{move(data_and_metadata.first)},
						  metadata{data_and_metadata.second},
						  node_address_map,
						  output_cache](int thread_id) {
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
					buffer_transport transport(node_address_map, metadata, buffer_sizes, column_transports);
					for(size_t i = 0; i < raw_buffers.size(); i++) {
						transport.send(raw_buffers[i], buffer_sizes[i]);
						// temp_scope_holder[buffer_index] = nullptr;	// TODO: allow the device_vector to go out of
						// scope
					}
					transport.wait_until_complete();  // ensures that the message has been sent before returning the thread
												   // to the pool
				} catch(auto & e) {
				}
			});
		}
	}

	ctpl::thread_pool<BlazingThread> pool;
	std::shared_ptr<ral::cache::CacheMachine> output_cache;
	std::map<std::string, blazingdb::transport::Node> node_address_map;
};

}  // namespace comm
