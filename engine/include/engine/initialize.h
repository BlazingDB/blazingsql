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
	std::map<std::string, std::string> config_options);

void finalize();

void blazingSetAllocator(
	std::string allocation_mode,
	std::size_t initial_pool_size,
	std::map<std::string, std::string> config_options);

extern "C" {

error_code_t initialize_C(int ralId,
	int gpuId,
	std::string network_iface_name,
	std::string ralHost,
	int ralCommunicationPort,
	bool singleNode,
	std::map<std::string, std::string> config_options);

error_code_t finalize_C();

error_code_t blazingSetAllocator_C(
	std::string allocation_mode,
	std::size_t initial_pool_size,
	std::map<std::string, std::string> config_options);

} // extern "C"
