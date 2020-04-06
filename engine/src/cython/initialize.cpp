#include <stdio.h>
#include <string.h> /* for strncpy */
#include <unistd.h>
#include <clocale>

#include <arpa/inet.h>
#include <net/if.h>
#include <netinet/in.h>
#include <sys/ioctl.h>
#include <sys/socket.h>
#include <sys/types.h>


#include <algorithm>
#include <cuda_runtime.h>
#include <memory>
#include <thread>


#include <blazingdb/transport/io/reader_writer.h>


#include <blazingdb/io/Util/StringUtil.h>

#include <blazingdb/io/Config/BlazingContext.h>
#include <blazingdb/io/Library/Logging/FileOutput.h>
#include <blazingdb/io/Library/Logging/Logger.h>
#include "blazingdb/io/Library/Logging/ServiceLogging.h"
#include "utilities/StringUtils.h"

#include "Traits/RuntimeTraits.h"


#include "config/BlazingConfig.h"
#include "config/GPUManager.cuh"

#include "communication/CommunicationData.h"
#include "communication/network/Client.h"
#include "communication/network/Server.h"
#include <blazingdb/manager/Context.h>


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

void initialize(int ralId,
	int gpuId,
	std::string network_iface_name,
	std::string ralHost,
	int ralCommunicationPort,
	bool singleNode) {
  // ---------------------------------------------------------------------------
  // DISCLAIMER
  // TODO: Support proper locale support for non-US cases (percy)
  std::setlocale(LC_ALL, "en_US.UTF-8");
  std::setlocale(LC_NUMERIC, "en_US.UTF-8");
  // ---------------------------------------------------------------------------

	ralHost = get_ip(network_iface_name);

	std::string loggingName = "RAL." + std::to_string(ralId) + ".log";
	
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
	blazingdb::transport::experimental::io::setPinnedBufferProvider(0.1 * total_gpu_mem_size, nthread);

	auto & communicationData = ral::communication::experimental::CommunicationData::getInstance();
	communicationData.initialize(ralId, "1.1.1.1", 0, ralHost, ralCommunicationPort, 0);

	ral::communication::network::experimental::Server::start(ralCommunicationPort);

	if(singleNode == true) {
		ral::communication::network::experimental::Server::getInstance().close();
	}
	auto & config = ral::config::BlazingConfig::getInstance();

	// NOTE IMPORTANT PERCY aqui es que pyblazing se entera que este es el ip del RAL en el _send de pyblazing
	config.setLogName(loggingName).setSocketPath(ralHost);

	auto output = new Library::Logging::FileOutput(config.getLogName(), false);
	Library::Logging::ServiceLogging::getInstance().setLogOutput(output);
	Library::Logging::ServiceLogging::getInstance().setNodeIdentifier(ralId);
	
	Library::Logging::Logger().logTrace(ral::utilities::buildLogString("0","0","0",
		initLogMsg));

	// Init AWS S3 ... TODO see if we need to call shutdown and avoid leaks from s3 percy
	BlazingContext::getInstance()->initExternalSystems();
}

void finalize() {
	ral::communication::network::experimental::Client::closeConnections();
	ral::communication::network::experimental::Server::getInstance().close();
	cudaDeviceReset();
	exit(0);
}
