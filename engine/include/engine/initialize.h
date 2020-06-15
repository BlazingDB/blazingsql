#include <string>
#include <vector>
#include <map>

void initialize(int ralId,
	int gpuId,
	std::string network_iface_name,
	std::string ralHost,
	int ralCommunicationPort,
	bool singleNode,
	std::map<std::string, std::string> config_options);

void finalize();

void blazingSetAllocator(
	std::string allocation_mode, 
	std::size_t initial_pool_size, 
	std::map<std::string, std::string> config_options);
	