#include <string>
#include <vector>
#include <map>
#include <memory>
#include <cstdint>
#include "../../src/error.hpp"

#include "common.h"
#include "execution_graph/logic_controllers/cache_machine/CacheMachine.h"

std::pair<std::pair<std::shared_ptr<ral::cache::CacheMachine>,std::shared_ptr<ral::cache::CacheMachine> >, int> initialize(uint16_t ralId,
	std::string worker_id,
	std::string network_iface_name,
	int ralCommunicationPort,
	std::vector<NodeMetaDataUCP> workers_ucp_info,
	bool singleNode,
	std::map<std::string, std::string> config_options,
	std::string allocation_mode,
	std::size_t initial_pool_size,
	std::size_t maximum_pool_size,
	bool enable_logging);

void finalize(std::vector<int32_t> ctx_tokens);

size_t getFreeMemory();
void resetMaxMemoryUsed(int to = 0);
size_t getMaxMemoryUsed();

extern "C" {

error_code_t initialize_C(uint16_t ralId,
	std::string worker_id,
	std::string network_iface_name,
	int ralCommunicationPort,
	std::vector<NodeMetaDataUCP> workers_ucp_info,
	bool singleNode,
	std::map<std::string, std::string> config_options,
	std::string allocation_mode,
	std::size_t initial_pool_size,
	std::size_t maximum_pool_size,
	bool enable_logging);

error_code_t finalize_C(std::vector<int32_t> ctx_tokens);

} // extern "C"
