#pragma once

#include <transport/ColumnTransport.h>
#include <memory>
#include <rmm/device_buffer.hpp>
#include <utility>
#include <Util/StringUtil.h>

#include "ExceptionHandling/BlazingThread.h"
#include "node.hpp"
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
	 * @param context The ucp_context_h
	 * @param origin The ucp_worker_h
	 * @param ral_id The ral_id
	 * @param protocol The comm::blazing_protocol 
	 */
	message_sender(std::shared_ptr<ral::cache::CacheMachine> output_cache,
		const std::map<std::string, node> & node_address_map,
		int num_threads,
		ucp_context_h context,
		ucp_worker_h origin,
		int ral_id,
		comm::blazing_protocol protocol,
		bool require_acknowledge);

	static void initialize_instance(std::shared_ptr<ral::cache::CacheMachine> output_cache,
		std::map<std::string, node> node_address_map,
		int num_threads,
		ucp_context_h context,
		ucp_worker_h origin_node,
		int ral_id,
		comm::blazing_protocol protocol,
    	bool require_acknowledge);

	std::shared_ptr<ral::cache::CacheMachine> get_output_cache(){
		return output_cache;
	}
	
	/**
	 * @brief A polling function that listens on a cache for data and send it off via some protocol
	 */
	void run_polling();
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
	bool require_acknowledge;
};

}  // namespace comm
