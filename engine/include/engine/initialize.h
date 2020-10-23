#include <string>
#include <vector>
#include <map>
#include "../../src/error.hpp"

void initialize(int ralId,
	int gpuId,
	std::string network_iface_name,
	std::string ralHost,
	int ralCommunicationPort,
	bool singleNode,
	std::map<std::string, std::string> config_options,
	std::string allocation_mode,
	std::size_t initial_pool_size,
	std::size_t maximum_pool_size,
	bool enable_logging);

void finalize();

size_t getFreeMemory();

extern "C" {

error_code_t initialize_C(int ralId,
	int gpuId,
	std::string network_iface_name,
	std::string ralHost,
	int ralCommunicationPort,
	bool singleNode,
	std::map<std::string, std::string> config_options,
	std::string allocation_mode,
	std::size_t initial_pool_size,
	std::size_t maximum_pool_size,
	bool enable_logging);

error_code_t finalize_C();

} // extern "C"
