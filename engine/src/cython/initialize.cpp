#include <unistd.h>
#include <clocale>

#include <arpa/inet.h>
#include <net/if.h>
#include <sys/ioctl.h>
#include <sys/stat.h>
#include <execinfo.h>
#include <signal.h>

#include <ucp/api/ucp.h>

#include <spdlog/spdlog.h>
#include <spdlog/async.h>
#include <spdlog/sinks/basic_file_sink.h>
#include <spdlog/sinks/stdout_color_sinks.h>

#include <algorithm>
#include <cuda_runtime.h>
#include <memory>
#include <chrono>
#include <fstream>
#include <utility>
#include <memory>

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


#include "communication/CommunicationInterface/node.hpp"
#include "communication/CommunicationInterface/protocols.hpp"
#include "communication/CommunicationInterface/messageSender.hpp"
#include "communication/CommunicationInterface/messageListener.hpp"


#include "error.hpp"

using namespace fmt::literals;

#include "execution_graph/logic_controllers/CacheMachine.h"

#include "engine/initialize.h"
#include "engine/static.h" // this contains function call for getProductDetails

#include <sys/socket.h>
#include <sys/types.h>
#include <netinet/in.h>
#include <netdb.h>

using namespace fmt::literals;
using namespace ral::cache;

void handler(int sig) {
  void *array[10];
  size_t size;
  // get void*'s for all entries on the stack
  size = backtrace(array, 10);
  // print out all the frames to stderr
  fprintf(stderr, "Error: signal %d:\n", sig);
  backtrace_symbols_fd(array, size, STDERR_FILENO);
  exit(1);
}

std::string get_ip(const std::string & iface_name = "eno1") {
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

// simple_log: true (no timestamp or log level)
void create_logger(std::string fileName, std::string loggingName, int ralId, std::string flush_level, bool simple_log=true){
	auto stdout_sink = std::make_shared<spdlog::sinks::stdout_color_sink_mt>();
	stdout_sink->set_pattern("[%T.%e] [%^%l%$] %v");
	stdout_sink->set_level(spdlog::level::err);
	auto file_sink = std::make_shared<spdlog::sinks::basic_file_sink_mt>(fileName);
	if(simple_log){
		file_sink->set_pattern(fmt::format("%v"));
	}else{
		file_sink->set_pattern(fmt::format("%Y-%m-%d %T.%e|{}|%^%l%$|%v", ralId));
	}

	file_sink->set_level(spdlog::level::trace);
	spdlog::sinks_init_list sink_list = { stdout_sink, file_sink };
	auto logger = std::make_shared<spdlog::async_logger>(loggingName, sink_list, spdlog::thread_pool(), spdlog::async_overflow_policy::block);
	logger->set_level(spdlog::level::trace);
	spdlog::register_logger(logger);

	// type of flush
	if (flush_level == "critical") {
		spdlog::flush_on(spdlog::level::critical);
	}
	else if (flush_level == "error") {
		spdlog::flush_on(spdlog::level::err);
	}
	else if (flush_level == "info") {
		spdlog::flush_on(spdlog::level::info);
	}
	else if (flush_level == "debug") {
		spdlog::flush_on(spdlog::level::debug);
	}
	else if (flush_level == "trace") {
		spdlog::flush_on(spdlog::level::trace);
	}
	else {
		spdlog::flush_on(spdlog::level::warn);
	}

	spdlog::flush_every(std::chrono::seconds(1));
}

//=============================================================================

template <class Callable>
static inline void CheckError(const bool condition,
                              const std::string &message,
                              Callable &&callable) {
  if (condition) {
    std::forward<Callable>(callable)();
    std::cerr << message << std::endl;
    throw std::runtime_error(message);
  }
}

static inline void CheckError(const bool condition,
                              const std::string &message) {
  CheckError(condition, message, []() {});
}

//- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

class UcpWorkerAddress {
public:
  ucp_address_t *address;
  std::size_t length;
};

//- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

class AddressExchanger {
public:
  const UcpWorkerAddress Exchange(const UcpWorkerAddress &ucpWorkerAddress) {

    std::uint8_t *data = new std::uint8_t[ucpWorkerAddress.length];

    UcpWorkerAddress peerUcpWorkerAddress{
        reinterpret_cast<ucp_address_t *>(data),
        std::numeric_limits<decltype(ucpWorkerAddress.length)>::max()};

    try {
      Exchange(&peerUcpWorkerAddress.length,
               fd(),
               &ucpWorkerAddress.length,
               sizeof(ucpWorkerAddress.length));

      Exchange(peerUcpWorkerAddress.address,
               fd(),
               ucpWorkerAddress.address,
               ucpWorkerAddress.length);
    } catch (...) {
      delete[] data;
      throw;
    }

    return peerUcpWorkerAddress;
  }

  static std::unique_ptr<AddressExchanger>
  MakeForSender(const std::uint16_t port);

  static std::unique_ptr<AddressExchanger>
  MakeForReceiver(const std::uint16_t port, const char *hostname);

protected:
  virtual int fd() = 0;

private:
  static inline void Exchange(void *peerData,
                              const int fd,
                              const void *localData,
                              const std::size_t length) {
    int ret = send(fd, localData, length, 0);
    CheckError(ret != static_cast<int>(length), "send");
    ret = recv(fd, peerData, length, MSG_WAITALL);
    CheckError(ret != static_cast<int>(length), "recv");
  }
};

class AddressExchangerForSender : public AddressExchanger {
public:
  ~AddressExchangerForSender() { CheckError(close(fd()), "close sender"); }

  AddressExchangerForSender(const std::uint16_t port) {
    struct sockaddr_in inaddr;

    int lsock = -1;
    int dsock = -1;
    int optval = 1;
    int ret;

    lsock = socket(AF_INET, SOCK_STREAM, 0);
    if (lsock < 0) {
      std::cerr << "open server socket" << std::endl;
      throw std::runtime_error("open server socket");
    }

    optval = 1;
    ret = setsockopt(lsock, SOL_SOCKET, SO_REUSEADDR, &optval, sizeof(optval));
    if (ret < 0) {
      std::cerr << "server setsockopt()" << std::endl;
      close(lsock);
      throw std::runtime_error("server setsockopt()");
    }

    inaddr.sin_family = AF_INET;
    inaddr.sin_port = htons(port);
    inaddr.sin_addr.s_addr = INADDR_ANY;
    std::memset(inaddr.sin_zero, 0, sizeof(inaddr.sin_zero));
    ret = bind(lsock, (struct sockaddr *) &inaddr, sizeof(inaddr));
    if (ret < 0) {
      std::cout << "bind server" << std::endl;
      close(lsock);
      throw std::runtime_error("bind server");
    }

    ret = listen(lsock, 0);
    if (ret < 0) {
      std::cout << "listen server" << std::endl;
      close(lsock);
      throw std::runtime_error("listen server");
    }

    std::cout << "[" << std::hex << std::this_thread::get_id()
              << "] Waiting for connection..." << std::endl;

    dsock = accept(lsock, NULL, NULL);
    if (dsock < 0) {
      std::cout << "accept server" << std::endl;
      close(lsock);
      throw std::runtime_error("accept server");
    }

    close(lsock);

    CheckError(dsock < 0, "server_connect");
    dsock_ = dsock;
  }

protected:
  int fd() final { return dsock_; }

private:
  int dsock_;
};

class AddressExchangerForReceiver : public AddressExchanger {
public:
  ~AddressExchangerForReceiver() { CheckError(close(fd()), "close receiver"); }

  AddressExchangerForReceiver(const std::uint16_t port, const char *hostname) {
    struct sockaddr_in conn_addr;
    struct hostent *he;
    int connfd;
    int ret;

    connfd = socket(AF_INET, SOCK_STREAM, 0);
    if (connfd < 0) {
      const std::string message = "open client socket";
      std::cout << message << std::endl;
      throw std::runtime_error(message);
    }

    he = gethostbyname(hostname);
    if (he == NULL || he->h_addr_list == NULL) {
      const std::string message = "found a host";
      std::cout << message << std::endl;
      close(connfd);
      throw std::runtime_error(message);
    }

    conn_addr.sin_family = he->h_addrtype;
    conn_addr.sin_port = htons(port);

    std::memcpy(&conn_addr.sin_addr, he->h_addr_list[0], he->h_length);
    std::memset(conn_addr.sin_zero, 0, sizeof(conn_addr.sin_zero));

    ret = connect(connfd, (struct sockaddr *) &conn_addr, sizeof(conn_addr));
    if (ret < 0) {
      const std::string message = "connect client";
      std::cout << message << std::endl;
      close(connfd);
      throw std::runtime_error(message);
    }

    CheckError(connfd < 0, "server_connect");
    connfd_ = connfd;
  }

protected:
  int fd() final { return connfd_; }

private:
  int connfd_;
};

std::unique_ptr<AddressExchanger>
AddressExchanger::MakeForSender(const std::uint16_t port) {
  return std::make_unique<AddressExchangerForSender>(port);
}

std::unique_ptr<AddressExchanger>
AddressExchanger::MakeForReceiver(const std::uint16_t port,
                                  const char *hostname) {
  return std::make_unique<AddressExchangerForReceiver>(port, hostname);
}

static void request_init(void *request)
{
  struct comm::ucx_request *req = (comm::ucx_request *)request;
  req->completed = 0;
  req->uid = -1;
}

//- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

ucp_context_h CreateUcpContext() {
  ucp_config_t *config;
  ucs_status_t status = ucp_config_read(NULL, NULL, &config);
  CheckError(status != UCS_OK, "ucp_config_read");

  ucp_params_t ucp_params;
  std::memset(&ucp_params, 0, sizeof(ucp_params));
  ucp_params.field_mask = UCP_PARAM_FIELD_FEATURES |
                          UCP_PARAM_FIELD_REQUEST_SIZE |
                          UCP_PARAM_FIELD_REQUEST_INIT;
  ucp_params.features = UCP_FEATURE_TAG | UCP_FEATURE_WAKEUP;
  ucp_params.request_size = sizeof(comm::ucx_request);
  ucp_params.request_init = request_init;

  ucp_context_h ucp_context;
  status = ucp_init(&ucp_params, config, &ucp_context);

  const bool hasPrintUcpConfig = false;
  if (hasPrintUcpConfig) {
    ucp_config_print(config, stdout, NULL, UCS_CONFIG_PRINT_CONFIG);
  }

  ucp_config_release(config);
  CheckError(status != UCS_OK, "ucp_init");

  return ucp_context;
}

ucp_worker_h CreatetUcpWorker(ucp_context_h ucp_context) {
  ucp_worker_params_t worker_params;
  std::memset(&worker_params, 0, sizeof(worker_params));
  worker_params.field_mask = UCP_WORKER_PARAM_FIELD_THREAD_MODE;
  worker_params.thread_mode = UCS_THREAD_MODE_MULTI;  // UCS_THREAD_MODE_SINGLE;

  ucp_worker_h ucp_worker;
  ucs_status_t status =
      ucp_worker_create(ucp_context, &worker_params, &ucp_worker);
  CheckError(status != UCS_OK, "ucp_worker_create", [&ucp_context]() {
    ucp_cleanup(ucp_context);
  });

  return ucp_worker;
}

UcpWorkerAddress GetUcpWorkerAddress(ucp_worker_h ucp_worker) {
  UcpWorkerAddress ucpWorkerAddress;

  ucs_status_t status = ucp_worker_get_address(
      ucp_worker, &ucpWorkerAddress.address, &ucpWorkerAddress.length);
  CheckError(status != UCS_OK,
             "ucp_worker_get_address",
             [&ucp_worker /*, &ucp_context*/]() {
               ucp_worker_destroy(ucp_worker);
               // ucp_cleanup(ucp_context);
             });

  return ucpWorkerAddress;
}

static class ErrorHandling {
public:
  ucp_err_handling_mode_t ucp_err_mode;
  int failure;
} err_handling_opt;

static void failure_handler(void *arg, ucp_ep_h, ucs_status_t status) {
  ucs_status_t *arg_status = static_cast<ucs_status_t *>(arg);
  std::cout << '[' << std::hex << std::this_thread::get_id()
            << "] failure handler called with status " << status << std::endl;
  *arg_status = status;
}

ucp_ep_h CreateUcpEp(ucp_worker_h ucp_worker,
                     const UcpWorkerAddress &ucpWorkerAddress) {
  static ucs_status_t current_status = UCS_OK;
  ucp_ep_params_t ep_params;
  ep_params.field_mask =
      UCP_EP_PARAM_FIELD_REMOTE_ADDRESS | UCP_EP_PARAM_FIELD_ERR_HANDLING_MODE |
      UCP_EP_PARAM_FIELD_ERR_HANDLER | UCP_EP_PARAM_FIELD_USER_DATA;
  ep_params.address = ucpWorkerAddress.address;
  ep_params.err_mode = err_handling_opt.ucp_err_mode;
  ep_params.err_handler.cb = failure_handler;
  ep_params.err_handler.arg = NULL;
  ep_params.user_data = &current_status;

  ucp_ep_h ucp_ep;
  ucs_status_t status = ucp_ep_create(ucp_worker, &ep_params, &ucp_ep);
  CheckError(status != UCS_OK, "ucp_ep_create");

  return ucp_ep;
}

//=============================================================================

/**
* Initializes the engine and gives us shared pointers to both our transport out cache
* and the cache we use for receiving messages
*
*/
std::pair<std::shared_ptr<CacheMachine>,std::shared_ptr<CacheMachine> > initialize(int ralId,
	std::string worker_id,
	int gpuId,
	std::string network_iface_name,
	std::string ralHost,
	int ralCommunicationPort,
	std::vector<NodeMetaDataUCP> workers_ucp_info,
	bool singleNode,
	std::map<std::string, std::string> config_options) {
	// ---------------------------------------------------------------------------
	// DISCLAIMER
	// TODO: Support proper locale support for non-US cases (percy)
	std::setlocale(LC_ALL, "en_US.UTF-8");
	std::setlocale(LC_NUMERIC, "en_US.UTF-8");
	// ---------------------------------------------------------------------------

	signal(SIGSEGV, handler);   // install our handler

	ralHost = get_ip(network_iface_name);

	std::string initLogMsg = "INITIALIZING RAL. RAL ID: " + std::to_string(ralId)  + ", ";
	initLogMsg = initLogMsg + "RAL Host: " + ralHost + ":" + std::to_string(ralCommunicationPort) + ", ";
	initLogMsg = initLogMsg + "Network Interface: " + network_iface_name + ", ";
	initLogMsg = initLogMsg + (singleNode ? ", Is Single Node, " : ", Is Not Single Node, ");

	const char * env_cuda_device = std::getenv("CUDA_VISIBLE_DEVICES");
	std::string env_cuda_device_str = env_cuda_device == nullptr ? "" : std::string(env_cuda_device);
	initLogMsg = initLogMsg + "CUDA_VISIBLE_DEVICES is set to: " + env_cuda_device_str + ", ";

	size_t total_gpu_mem_size = ral::config::gpuTotalMemory();
	assert(total_gpu_mem_size > 0);
	auto nthread = 4;
	blazingdb::transport::io::setPinnedBufferProvider(0.1 * total_gpu_mem_size, nthread);

	//to avoid redundancy the default value or user defined value for this parameter is placed on the pyblazing side
	assert( config_options.find("BLAZ_HOST_MEM_CONSUMPTION_THRESHOLD") != config_options.end() );
	float host_memory_quota = std::stof(config_options["BLAZ_HOST_MEM_CONSUMPTION_THRESHOLD"]);

	blazing_host_memory_resource::getInstance().initialize(host_memory_quota);


	// Init AWS S3 ... TODO see if we need to call shutdown and avoid leaks from s3 percy
	BlazingContext::getInstance()->initExternalSystems();

	// spdlog batch logger
	spdlog::shutdown();

	spdlog::init_thread_pool(8192, 1);

	std::string flush_level = "warn";
	auto log_it = config_options.find("LOGGING_LEVEL");
	if (log_it != config_options.end()){
		flush_level = config_options["LOGGING_LEVEL"];
	}

	// type of flush
	if (flush_level == "critical") {
		spdlog::flush_on(spdlog::level::critical);
	}
	else if (flush_level == "error") {
		spdlog::flush_on(spdlog::level::err);
	}
	else if (flush_level == "info") {
		spdlog::flush_on(spdlog::level::info);
	}
	else if (flush_level == "debug") {
		spdlog::flush_on(spdlog::level::debug);
	}
	else if (flush_level == "trace") {
		spdlog::flush_on(spdlog::level::trace);
	}
	else {
		spdlog::flush_on(spdlog::level::warn);
	}

	spdlog::flush_every(std::chrono::seconds(1));

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

	std::string batchLoggerFileName = logging_dir + "/RAL." + std::to_string(ralId) + ".log";
	create_logger(batchLoggerFileName, "batch_logger", ralId, flush_level, false);
	std::cout<<batchLoggerFileName<<std::endl;

	std::string queriesFileName = logging_dir + "/bsql_queries." + std::to_string(ralId) + ".log";
	bool existsQueriesFileName = std::ifstream(queriesFileName).good();
	create_logger(queriesFileName, "queries_logger", ralId, flush_level);

	std::string kernelsFileName = logging_dir + "/bsql_kernels." + std::to_string(ralId) + ".log";
	bool existsKernelsFileName = std::ifstream(kernelsFileName).good();
	create_logger(kernelsFileName, "kernels_logger", ralId, flush_level);

	std::string kernelsEdgesFileName = logging_dir + "/bsql_kernels_edges." + std::to_string(ralId) + ".log";
	bool existsKernelsEdgesFileName = std::ifstream(kernelsEdgesFileName).good();
	create_logger(kernelsEdgesFileName, "kernels_edges_logger", ralId, flush_level);

	std::string kernelEventsFileName = logging_dir + "/bsql_kernel_events." + std::to_string(ralId) + ".log";
	bool existsKernelEventsFileName = std::ifstream(kernelEventsFileName).good();
	create_logger(kernelEventsFileName, "events_logger", ralId, flush_level);

	std::string cacheEventsFileName = logging_dir + "/bsql_cache_events." + std::to_string(ralId) + ".log";
	bool existsCacheEventsFileName = std::ifstream(cacheEventsFileName).good();
	create_logger(cacheEventsFileName, "cache_events_logger", ralId, flush_level);

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
	std::cout<<"getting product details!!!"<<std::endl;
	std::map<std::string, std::string> product_details = getProductDetails();
	std::string product_details_str = "Product Details: ";
	std::map<std::string, std::string>::iterator it = product_details.begin();
	while (it != product_details.end())	{
		product_details_str += it->first + ": " + it->second + "; ";
		it++;
	}
	logger->debug("|||{info}|||||","info"_a=product_details_str);


	auto & communicationData = ral::communication::CommunicationData::getInstance();

	auto output_input_caches = std::make_pair(std::make_shared<CacheMachine>(nullptr),std::make_shared<CacheMachine>(nullptr));

	// start ucp servers

	communicationData.initialize(worker_id, ralHost, ralCommunicationPort);

	std::cout<<"going to init comms!!!"<<std::endl;
	if(! singleNode){
		std::map<std::string, comm::node> nodes_info_map;
		// for (auto &&node_data : workers_ucp_info) {
		// 	std::cout<<"ep handle is "<< node_data.ep_handle<<" and worker handle is "<< node_data.worker_handle<<std::endl;
		// 	nodes_info_map.emplace(node_data.worker_id, comm::node(ralId, node_data.worker_id, reinterpret_cast<ucp_ep_h>(node_data.ep_handle), reinterpret_cast<ucp_worker_h>(node_data.worker_handle)));
		// }

		std::cout<<"getting worker"<<worker_id<<std::endl;
		std::cout<<"initializing listener"<<std::endl;

		// ucp_context_h ucp_context = reinterpret_cast<ucp_context_h>(workers_ucp_info[0].context_handle);
		// ucp_worker_h self_worker = reinterpret_cast<ucp_worker_h>(workers_ucp_info[0].worker_handle);


		// ucp_context_h ucp_context = CreateUcpContext();
		ucp_context_h ucp_context = reinterpret_cast<ucp_context_h>(workers_ucp_info[0].context_handle);

		std::cout<<">>>>>> CREATED CONTEXT"<<std::endl;

		ucp_worker_h self_worker = CreatetUcpWorker(ucp_context);

		std::cout<<">>>>>> CREATED WORKER"<<std::endl;

		UcpWorkerAddress ucpWorkerAddress = GetUcpWorkerAddress(self_worker);

		std::cout<<">>>>>> GOT LOCAL WORKER ADDRESS"<<std::endl;

		std::unique_ptr<AddressExchanger> addressExchanger;
		if (ralId == 0) {
			addressExchanger = AddressExchanger::MakeForSender(13337);
		} else {
			std::this_thread::sleep_for(std::chrono::seconds(1));
			addressExchanger = AddressExchanger::MakeForReceiver(13337, "10.0.0.232");
		}

		std::cout<<">>>>>> CREATED EXCHANGER"<<std::endl;

		UcpWorkerAddress peerUcpWorkerAddress =
				addressExchanger->Exchange(ucpWorkerAddress);

		std::cout<<">>>>>> EXACHNAGE DONE"<<std::endl;

  	ucp_ep_h ucp_ep = CreateUcpEp(self_worker, peerUcpWorkerAddress);

		std::cout<<">>>>>> CREATED ENDPOINT"<<std::endl;

		std::cout << '[' << std::hex << std::this_thread::get_id()
							<< "] local: " << std::hex
							<< *reinterpret_cast<std::size_t *>(ucpWorkerAddress.address) << ' '
							<< ucpWorkerAddress.length << std::endl
							<< '[' << std::hex << std::this_thread::get_id()
							<< "] peer: " << std::hex
							<< *reinterpret_cast<std::size_t *>(peerUcpWorkerAddress.address)
							<< ' ' << peerUcpWorkerAddress.length << std::endl;


		std::cout<<">>>>>> WORKER INFO SIZE "<< workers_ucp_info.size()<<std::endl;

		auto node_data = workers_ucp_info[0];
		nodes_info_map.emplace(node_data.worker_id, comm::node(ralId, node_data.worker_id, ucp_ep, self_worker));

		comm::blazing_protocol protocol = comm::blazing_protocol::ucx;
		if(config_options.find("PROTOCOL") != config_options.end()){
			if(config_options["PROTOCOL"] == "UCX"){
				protocol = comm::blazing_protocol::ucx;
			}
		}

		if(protocol == comm::blazing_protocol::ucx){
			comm::ucx_message_listener::initialize_message_listener(
				ucp_context, self_worker,nodes_info_map,20);
			std::cout<<"starting polling"<<std::endl;
			comm::ucx_message_listener::get_instance()->poll_begin_message_tag(true);

			std::cout<<"initializing sender"<<std::endl;

			comm::message_sender::initialize_instance(output_input_caches.first,
				nodes_info_map,
				20, ucp_context, self_worker, ralId,comm::blazing_protocol::ucx);
			std::cout<<"starting polling sender"<<std::endl;


		}else{

			comm::tcp_message_listener::initialize_message_listener(nodes_info_map,ralCommunicationPort,20);
			std::cout<<"initializing sender"<<std::endl;
			comm::tcp_message_listener::get_instance()->start_polling();

			comm::message_sender::initialize_instance(output_input_caches.first,
				nodes_info_map,
				20, ucp_context, self_worker, ralId,comm::blazing_protocol::tcp);
			std::cout<<"starting polling sender"<<std::endl;

		}

			comm::message_sender::get_instance()->run_polling();


	}
		std::cout<<"finish comms init!!!"<<std::endl;

	//TODO: make this number configurable in options



	return output_input_caches;

}

void finalize() {

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
	std::string worker_id,
	int gpuId,
	std::string network_iface_name,
	std::string ralHost,
	int ralCommunicationPort,
	std::vector<NodeMetaDataUCP> workers_ucp_info,
	bool singleNode,
	std::map<std::string, std::string> config_options) {

	try {
		initialize(ralId,
			worker_id,
			gpuId,
			network_iface_name,
			ralHost,
			ralCommunicationPort,
			workers_ucp_info,
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
