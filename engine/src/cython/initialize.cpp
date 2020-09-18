#include <unistd.h>
#include <clocale>

#include <arpa/inet.h>
#include <net/if.h>
#include <sys/ioctl.h>
#include <sys/stat.h>

#include <spdlog/spdlog.h>
#include <spdlog/async.h>
#include <spdlog/sinks/basic_file_sink.h>
#include <spdlog/sinks/stdout_color_sinks.h>

#include <algorithm>
#include <cuda_runtime.h>
#include <memory>
#include <chrono>
#include <fstream>

#include <blazingdb/transport/io/reader_writer.h>

#include <blazingdb/io/Config/BlazingContext.h>
#include <blazingdb/io/Library/Logging/CoutOutput.h>
#include <blazingdb/io/Library/Logging/Logger.h>
#include "blazingdb/io/Library/Logging/ServiceLogging.h"
#include "utilities/StringUtils.h"

#include "config/GPUManager.cuh"

#include "communication/CommunicationData.h"
#include "communication/network/Client.h"
#include "communication/network/Server.h"
#include <bmr/initializer.h>
#include <bmr/BlazingMemoryResource.h>

#include "error.hpp"

using namespace fmt::literals;

#include <engine/static.h> // this contains function call for getProductDetails

std::string get_ip(const std::string & iface_name = "eth0") {
	int fd;
	struct ifreq ifr;

	fd = socket(AF_INET, SOCK_DGRAM, 0);

	/* I want to get an IPv4 IP address */
	ifr.ifr_addr.sa_family = AF_INET;

	/* I want IP address attached to "eth0" */
	strncpy(ifr.ifr_name, iface_name.c_str(), IFNAMSIZ - 1);

	ioctl(fd, SIOCGIFADDR, &ifr);

	close(fd);

	/* display result */
	// printf("%s\n", inet_ntoa(((struct sockaddr_in *)&ifr.ifr_addr)->sin_addr));
	const std::string the_ip(inet_ntoa(((struct sockaddr_in *) &ifr.ifr_addr)->sin_addr));

	return the_ip;
}



auto log_level_str_to_enum(std::string level) {
	if (level == "critical") {
		return spdlog::level::critical;
	}
	else if (level == "err") {
		return spdlog::level::err;
	}
	else if (level == "info") {
		return spdlog::level::info;
	}
	else if (level == "debug") {
		return spdlog::level::debug;
	}
	else if (level == "trace") {
		return spdlog::level::trace;		
	}
	else if (level == "warn") {
		return spdlog::level::warn;		
	}
	else {
		return spdlog::level::off;
	}
}

// simple_log: true (no timestamp or log level)
void create_logger(std::string fileName,
	std::string loggingName,
	int ralId, std::string flush_level,
	std::string logger_level_wanted,
	bool simple_log=true) {

	auto stdout_sink = std::make_shared<spdlog::sinks::stdout_color_sink_mt>();
	stdout_sink->set_pattern("[%T.%e] [%^%l%$] %v");
	stdout_sink->set_level(spdlog::level::err);
	auto file_sink = std::make_shared<spdlog::sinks::basic_file_sink_mt>(fileName);
	if(simple_log){
		file_sink->set_pattern(fmt::format("%v"));
	}else{
		file_sink->set_pattern(fmt::format("%Y-%m-%d %T.%e|{}|%^%l%$|%v", ralId));
	}

	// We want ALL levels of info to be registered. So using by default `trace` level
	file_sink->set_level(spdlog::level::trace);
	spdlog::sinks_init_list sink_list = { stdout_sink, file_sink };
	auto logger = std::make_shared<spdlog::async_logger>(loggingName, sink_list, spdlog::thread_pool(), spdlog::async_overflow_policy::block);

	// level of logs
	logger->set_level(log_level_str_to_enum(logger_level_wanted));
	
	spdlog::register_logger(logger);

	spdlog::flush_on(log_level_str_to_enum(flush_level));

	spdlog::flush_every(std::chrono::seconds(1));
}

void initialize(int ralId,
	int gpuId,
	std::string network_iface_name,
	std::string ralHost,
	int ralCommunicationPort,
	bool singleNode,
	std::map<std::string, std::string> config_options) {
	// ---------------------------------------------------------------------------
	// DISCLAIMER
	// TODO: Support proper locale support for non-US cases (percy)
	std::setlocale(LC_ALL, "en_US.UTF-8");
	std::setlocale(LC_NUMERIC, "en_US.UTF-8");
	// ---------------------------------------------------------------------------

	ralHost = get_ip(network_iface_name);

	std::string initLogMsg = "INITIALIZING RAL. RAL ID: " + std::to_string(ralId)  + ", ";
	initLogMsg = initLogMsg + "RAL Host: " + ralHost + ":" + std::to_string(ralCommunicationPort) + ", ";
	initLogMsg = initLogMsg + "Network Interface: " + network_iface_name + ", ";
	initLogMsg = initLogMsg + (singleNode ? ", Is Single Node, " : ", Is Not Single Node, ");

	const char * env_cuda_device = std::getenv("CUDA_VISIBLE_DEVICES");
	std::string env_cuda_device_str = env_cuda_device == nullptr ? "" : std::string(env_cuda_device);
	initLogMsg = initLogMsg + "CUDA_VISIBLE_DEVICES is set to: " + env_cuda_device_str + ", ";
	
	size_t buffers_size = 78643200 / 8;  // 75 MBs / 8       0.1 * free_gpu_mem_size;
	auto iter = config_options.find("TRANSPORT_BUFFER_BYTE_SIZE");
	if (iter != config_options.end()){
		buffers_size = std::stoi(config_options["TRANSPORT_BUFFER_BYTE_SIZE"]);
	}
	int num_buffers = 20;
	iter = config_options.find("MAX_SEND_MESSAGE_THREADS");
	if (iter != config_options.end()){
		num_buffers = std::stoi(config_options["MAX_SEND_MESSAGE_THREADS"]);
	}	
	blazingdb::transport::io::setPinnedBufferProvider(buffers_size, num_buffers);

	//to avoid redundancy the default value or user defined value for this parameter is placed on the pyblazing side
	assert( config_options.find("BLAZ_HOST_MEM_CONSUMPTION_THRESHOLD") != config_options.end() );
	float host_memory_quota = std::stof(config_options["BLAZ_HOST_MEM_CONSUMPTION_THRESHOLD"]);

	blazing_host_memory_resource::getInstance().initialize(host_memory_quota);

	auto & communicationData = ral::communication::CommunicationData::getInstance();
	communicationData.initialize(ralId, "1.1.1.1", 0, ralHost, ralCommunicationPort, 0);

	ral::communication::network::Server::start(ralCommunicationPort, true);

	if(singleNode == true) {
		ral::communication::network::Server::getInstance().close();
	}

	// Init AWS S3 ... TODO see if we need to call shutdown and avoid leaks from s3 percy
	BlazingContext::getInstance()->initExternalSystems();

	// spdlog batch logger
	spdlog::shutdown();

	spdlog::init_thread_pool(8192, 1);

	std::string logging_dir = "blazing_log";
	auto config_it = config_options.find("BLAZING_LOGGING_DIRECTORY");
	if (config_it != config_options.end()){
		logging_dir = config_options["BLAZING_LOGGING_DIRECTORY"];
	}
	bool logging_directory_missing = false;
	struct stat sb;
	if (!(stat(logging_dir.c_str(), &sb) == 0 && S_ISDIR(sb.st_mode))){ // logging_dir does not exist
	// we are assuming that this logging directory was created by the python layer, because only the python layer can only target on directory creation per server
	// having all RALs independently trying to create a directory simulatenously can cause problems
		logging_directory_missing = true;
		logging_dir = "";
	}

	std::string flush_level = "warn";
	auto log_it = config_options.find("LOGGING_FLUSH_LEVEL");
	if (log_it != config_options.end()){
		flush_level = config_options["LOGGING_FLUSH_LEVEL"];
	}
	
	std::string logger_level_wanted = "trace";
	auto log_level_it = config_options.find("LOGGING_LEVEL");
	if (log_level_it != config_options.end()){
		logger_level_wanted = config_options["LOGGING_LEVEL"];
	}

	std::string batchLoggerFileName = logging_dir + "/RAL." + std::to_string(ralId) + ".log";
	create_logger(batchLoggerFileName, "batch_logger", ralId, flush_level, logger_level_wanted, false);

	std::string queriesFileName = logging_dir + "/bsql_queries." + std::to_string(ralId) + ".log";
	bool existsQueriesFileName = std::ifstream(queriesFileName).good();
	create_logger(queriesFileName, "queries_logger", ralId, flush_level, logger_level_wanted);

	std::string kernelsFileName = logging_dir + "/bsql_kernels." + std::to_string(ralId) + ".log";
	bool existsKernelsFileName = std::ifstream(kernelsFileName).good();
	create_logger(kernelsFileName, "kernels_logger", ralId, flush_level, logger_level_wanted);

	std::string kernelsEdgesFileName = logging_dir + "/bsql_kernels_edges." + std::to_string(ralId) + ".log";
	bool existsKernelsEdgesFileName = std::ifstream(kernelsEdgesFileName).good();
	create_logger(kernelsEdgesFileName, "kernels_edges_logger", ralId, flush_level, logger_level_wanted);

	std::string kernelEventsFileName = logging_dir + "/bsql_kernel_events." + std::to_string(ralId) + ".log";
	bool existsKernelEventsFileName = std::ifstream(kernelEventsFileName).good();
	create_logger(kernelEventsFileName, "events_logger", ralId, flush_level, logger_level_wanted);

	std::string cacheEventsFileName = logging_dir + "/bsql_cache_events." + std::to_string(ralId) + ".log";
	bool existsCacheEventsFileName = std::ifstream(cacheEventsFileName).good();
	create_logger(cacheEventsFileName, "cache_events_logger", ralId, flush_level, logger_level_wanted);

	//Logger Headers
	if(!existsQueriesFileName) {
		std::shared_ptr<spdlog::logger> queries_logger = spdlog::get("queries_logger");
		queries_logger->info("ral_id|query_id|start_time|plan");
	}

	if(!existsKernelsFileName) {
		std::shared_ptr<spdlog::logger> kernels_logger = spdlog::get("kernels_logger");
		kernels_logger->info("ral_id|query_id|kernel_id|is_kernel|kernel_type");
	}

	if(!existsKernelsEdgesFileName) {
		std::shared_ptr<spdlog::logger> kernels_edges_logger = spdlog::get("kernels_edges_logger");
		kernels_edges_logger->info("ral_id|query_id|source|sink");
	}

	if(!existsKernelEventsFileName) {
		std::shared_ptr<spdlog::logger> events_logger = spdlog::get("events_logger");
		events_logger->info("ral_id|query_id|kernel_id|input_num_rows|input_num_bytes|output_num_rows|output_num_bytes|event_type|timestamp_begin|timestamp_end");
	}

	if(!existsCacheEventsFileName) {
		std::shared_ptr<spdlog::logger> cache_events_logger = spdlog::get("cache_events_logger");
		cache_events_logger->info("ral_id|query_id|source|sink|num_rows|num_bytes|event_type|timestamp_begin|timestamp_end");
	}

	std::shared_ptr<spdlog::logger> logger = spdlog::get("batch_logger");

	if (logging_directory_missing){
		logger->error("|||{info}|||||","info"_a="BLAZING_LOGGING_DIRECTORY not found. It was not created.");
	}

	logger->debug("|||{info}|||||","info"_a=initLogMsg);

	std::map<std::string, std::string> product_details = getProductDetails();
	std::string product_details_str = "Product Details: ";
	std::map<std::string, std::string>::iterator it = product_details.begin();
	while (it != product_details.end())	{
		product_details_str += it->first + ": " + it->second + "; ";
		it++;
	}
	logger->debug("|||{info}|||||","info"_a=product_details_str);
}

void finalize() {
	ral::communication::network::Client::closeConnections();
	ral::communication::network::Server::getInstance().close();
	BlazingRMMFinalize();
	spdlog::shutdown();
	cudaDeviceReset();
	exit(0);
}

void blazingSetAllocator(
	std::string allocation_mode,
	std::size_t initial_pool_size,
	std::map<std::string, std::string> config_options) {

	float device_mem_resouce_consumption_thresh = 0.95;
	auto it = config_options.find("BLAZING_DEVICE_MEM_CONSUMPTION_THRESHOLD");
	if (it != config_options.end()){
		device_mem_resouce_consumption_thresh = std::stof(config_options["BLAZING_DEVICE_MEM_CONSUMPTION_THRESHOLD"]);
	}

	BlazingRMMInitialize(allocation_mode, initial_pool_size, device_mem_resouce_consumption_thresh);
}

error_code_t initialize_C(int ralId,
	int gpuId,
	std::string network_iface_name,
	std::string ralHost,
	int ralCommunicationPort,
	bool singleNode,
	std::map<std::string, std::string> config_options) {

	try {
		initialize(ralId,
			gpuId,
			network_iface_name,
			ralHost,
			ralCommunicationPort,
			singleNode,
			config_options);
		return E_SUCCESS;
	} catch (std::exception& e) {
		return E_EXCEPTION;
	}
}

error_code_t finalize_C() {
	try {
		finalize();
		return E_SUCCESS;
	} catch (std::exception& e) {
		return E_EXCEPTION;
	}
}

error_code_t blazingSetAllocator_C(
	std::string allocation_mode,
	std::size_t initial_pool_size,
	std::map<std::string, std::string> config_options) {

	try {
		blazingSetAllocator(allocation_mode,
			initial_pool_size,
			config_options);
		return E_SUCCESS;
	} catch (std::exception& e) {
		return E_EXCEPTION;
	}
}

size_t getFreeMemory() {
	BlazingMemoryResource* resource = &blazing_device_memory_resource::getInstance();
	size_t total_free_memory = resource->get_memory_limit() - resource->get_memory_used();
	return total_free_memory;
}
