#include <string>
#include <vector>
#include <map>
#include <memory>
#include <cstdint>

#include "execution_graph/logic_controllers/CacheMachine.h"

std::pair<std::shared_ptr<ral::cache::CacheMachine>,std::shared_ptr<ral::cache::CacheMachine> > initialize(int ralId,
	std::string worker_id,
	int gpuId,
	std::string network_iface_name,
	std::string ralHost,
	int ralCommunicationPort,
	std::map<std::string, std::uintptr_t> dask_addr_to_ucp_handle,
	bool singleNode,
	std::map<std::string, std::string> config_options);

void finalize();

void blazingSetAllocator(
	std::string allocation_mode,
	std::size_t initial_pool_size,
	std::map<std::string, std::string> config_options);
