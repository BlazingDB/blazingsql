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
// #include <blazingdb/io/Library/Logging/TcpOutput.h>
#include "blazingdb/io/Library/Logging/ServiceLogging.h"
// #include "blazingdb/io/Library/Network/NormalSyncSocket.h"

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

	// std::cout << "Using the network interface: " + network_iface_name << std::endl;
	ralHost = get_ip(network_iface_name);

	//  std::cout << "RAL ID: " << ralId << std::endl;
	//  std::cout << "GPU ID: " << gpuId << std::endl;

	//  std::cout << "RAL HTTP communication host: " << ralHost << std::endl;
	//  std::cout << "RAL HTTP communication port: " << ralCommunicationPort << std::endl;

	// std::string loggingHost = "";
	// std::string loggingPort = 0;
	std::string loggingName = "";
	// if (argc == 11) {
	//   loggingHost = std::string(argv[9]);
	//   loggingPort = std::string(argv[10]);
	//   std::cout << "Logging host: " << ralHost << std::endl;
	//   std::cout << "Logging port: " << ralCommunicationPort << std::endl;
	// } else {
	loggingName = "RAL." + std::to_string(ralId) + ".log";
	std::cout << "Logging to " << loggingName << std::endl;
	std::cout << "is singleNode? " << singleNode << std::endl;

	// }

	const char * env_cuda_device = std::getenv("CUDA_VISIBLE_DEVICES");
	gpuId = 0; // NOTE: This is the default value
	if (env_cuda_device){
		std::string cuda_devices(env_cuda_device);
		const bool has_cor = (cuda_devices.at(0) == '[');
		if (has_cor) {
			cuda_devices.replace(0,1,"");
			cuda_devices.pop_back();
		}
		std::vector<std::string> tokens = StringUtil::split(cuda_devices, ",");
		
		gpuId = std::atoi(tokens.at(0).c_str());
		std::cout << "CUDA_VISIBLE_DEVICES is set to: " << cuda_devices << std::endl;
	} else {
		std::cout << "CUDA_VISIBLE_DEVICES is not set, using default GPU: " << gpuId << std::endl;
	}
	ral::config::GPUManager::getInstance().initialize(gpuId);
	std::cout << "Using GPU: " << ral::config::GPUManager::getInstance().getDeviceId() << std::endl;

	size_t total_gpu_mem_size = ral::config::GPUManager::getInstance().gpuMemorySize();
	assert(total_gpu_mem_size > 0);
	auto nthread = 4;
	blazingdb::transport::io::setPinnedBufferProvider(0.1 * total_gpu_mem_size, nthread);

	auto & communicationData = ral::communication::CommunicationData::getInstance();
	communicationData.initialize(ralId, "1.1.1.1", 0, ralHost, ralCommunicationPort, 0);

	ral::communication::network::Server::start(ralCommunicationPort);

	if(singleNode == true) {
		ral::communication::network::Server::getInstance().close();
	}
	auto & config = ral::config::BlazingConfig::getInstance();

	// NOTE IMPORTANT PERCY aqui es que pyblazing se entera que este es el ip del RAL en el _send de pyblazing
	config.setLogName(loggingName).setSocketPath(ralHost);

	// std::cout << "Socket Name: " << config.getSocketPath() << std::endl;

	// if (loggingName != ""){
	auto output = new Library::Logging::FileOutput(config.getLogName(), false);
	Library::Logging::ServiceLogging::getInstance().setLogOutput(output);
	Library::Logging::ServiceLogging::getInstance().setNodeIdentifier(ralId);
	// } else {
	//   auto output = new Library::Logging::TcpOutput();
	//   std::shared_ptr<Library::Network::NormalSyncSocket> loggingSocket =
	//   std::make_shared<Library::Network::NormalSyncSocket>(); loggingSocket->connect(loggingHost.c_str(),
	//   loggingPort.c_str()); output.setSocket(loggingSocket);
	//   Library::Logging::ServiceLogging::getInstance().setLogOutput(output);
	// }

	// Init AWS S3 ... TODO see if we need to call shutdown and avoid leaks from s3 percy
	BlazingContext::getInstance()->initExternalSystems();
}

void finalize() {
	ral::communication::network::Client::closeConnections();
	ral::communication::network::Server::getInstance().close();
	cudaDeviceReset();
	exit(0);
}
