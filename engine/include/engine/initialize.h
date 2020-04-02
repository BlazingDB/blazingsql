#include <string>

void initialize(int ralId,
	int gpuId,
	std::string network_iface_name,
	std::string ralHost,
	int ralCommunicationPort,
	bool singleNode);

void finalize();

void blazingSetAllocator(std::string allocator, 
	bool pool, 
	int initial_pool_size, 
	bool enable_logging);