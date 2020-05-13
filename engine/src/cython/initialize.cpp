#include <stdio.h>
#include <string.h> /* for strncpy */
#include <unistd.h>
#include <clocale>

#include <arpa/inet.h>
#include <net/if.h>
#include <sys/ioctl.h>

#include <algorithm>
#include <cuda_runtime.h>
#include <memory>
#include <chrono>

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

#include <spdlog/spdlog.h>
#include <spdlog/async.h>
#include <spdlog/sinks/basic_file_sink.h>
#include <spdlog/sinks/stdout_color_sinks.h>

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

void create_logger(std::string fileName, std::string loggingName, int ralId){
	auto stdout_sink = std::make_shared<spdlog::sinks::stdout_color_sink_mt>();
	stdout_sink->set_pattern("[%T.%e] [%^%l%$] %v");
	stdout_sink->set_level(spdlog::level::err);
	auto file_sink = std::make_shared<spdlog::sinks::basic_file_sink_mt>(fileName);
	file_sink->set_pattern(fmt::format("%Y-%m-%d %T.%e|{}|%^%l%$|%v", ralId));
	file_sink->set_level(spdlog::level::trace);
	spdlog::sinks_init_list sink_list = { stdout_sink, file_sink };
	auto logger = std::make_shared<spdlog::async_logger>(loggingName, sink_list, spdlog::thread_pool(), spdlog::async_overflow_policy::block);
	logger->set_level(spdlog::level::trace);
	spdlog::register_logger(logger);

	spdlog::flush_on(spdlog::level::err);
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

	size_t total_gpu_mem_size = ral::config::gpuMemorySize();
	assert(total_gpu_mem_size > 0);
	auto nthread = 4;
	blazingdb::transport::io::setPinnedBufferProvider(0.1 * total_gpu_mem_size, nthread);

	auto & communicationData = ral::communication::CommunicationData::getInstance();
	communicationData.initialize(ralId, "1.1.1.1", 0, ralHost, ralCommunicationPort, 0);

	ral::communication::network::Server::start(ralCommunicationPort, true);

	if(singleNode == true) {
		ral::communication::network::Server::getInstance().close();
	}

	// auto output = new Library::Logging::CoutOutput();
	// Library::Logging::ServiceLogging::getInstance().setLogOutput(output);
	// Library::Logging::ServiceLogging::getInstance().setNodeIdentifier(ralId);
	// Library::Logging::Logger().logTrace(ral::utilities::buildLogString("0","0","0",	initLogMsg));

	// Init AWS S3 ... TODO see if we need to call shutdown and avoid leaks from s3 percy
	BlazingContext::getInstance()->initExternalSystems();

	// spdlog batch logger
	spdlog::shutdown();

	spdlog::init_thread_pool(8192, 1);

	std::string oldfileName = "RAL." + std::to_string(ralId) + ".log";
	create_logger(oldfileName, "batch_logger", ralId);

	std::string queriesFileName = "queries." + std::to_string(ralId) + ".log";
	create_logger(queriesFileName, "queries_logger", ralId);

	std::string kernelsFileName = "kernels." + std::to_string(ralId) + ".log";
	create_logger(kernelsFileName, "kernels_logger", ralId);

	std::string eventsFileName = "events." + std::to_string(ralId) + ".log";
	create_logger(eventsFileName, "events_logger", ralId);
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
	int allocation_mode, 
	std::size_t initial_pool_size, 
	std::vector<int> devices,
	bool enable_logging,
	std::map<std::string, std::string> config_options) {

	rmmOptions_t rmmValues;
	rmmValues.allocation_mode = static_cast<rmmAllocationMode_t>(allocation_mode);
	rmmValues.initial_pool_size = initial_pool_size;
	rmmValues.enable_logging = enable_logging;

	for (size_t i = 0; i < devices.size(); ++i)
		rmmValues.devices.push_back(devices[i]);

	float device_mem_resouce_consumption_thresh = 0.95;
	auto it = config_options.find("BLAZING_DEVICE_MEM_RESOURCE_CONSUMPTION_THRESHOLD");
	if (it != config_options.end()){
		device_mem_resouce_consumption_thresh = std::stof(config_options["BLAZING_DEVICE_MEM_RESOURCE_CONSUMPTION_THRESHOLD"]);
	}

	BlazingRMMInitialize(&rmmValues, device_mem_resouce_consumption_thresh);
}
