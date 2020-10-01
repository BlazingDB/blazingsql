#include <string>
#include <vector>
#include <map>
#include <memory>
#include <cstdint>
#include "../../src/error.hpp"

#include "common.h"
#include "execution_graph/logic_controllers/CacheMachine.h"

std::pair<std::shared_ptr<ral::cache::CacheMachine>,std::shared_ptr<ral::cache::CacheMachine> > initialize(int ralId,
	std::string worker_id,
	int gpuId,
	std::string network_iface_name,
	int ralCommunicationPort,
	std::vector<NodeMetaDataUCP> workers_ucp_info,
	bool singleNode,
	std::map<std::string, std::string> config_options);

void finalize();

void blazingSetAllocator(
	std::string allocation_mode,
	std::size_t initial_pool_size,
	std::map<std::string, std::string> config_options);

extern "C" {

error_code_t initialize_C(int ralId,
	std::string worker_id,
	int gpuId,
	std::string network_iface_name,
	int ralCommunicationPort,
	std::vector<NodeMetaDataUCP> workers_ucp_info,
	bool singleNode,
	std::map<std::string, std::string> config_options);

error_code_t finalize_C();

error_code_t blazingSetAllocator_C(
	std::string allocation_mode,
	std::size_t initial_pool_size,
	std::map<std::string, std::string> config_options);

} // extern "C"
