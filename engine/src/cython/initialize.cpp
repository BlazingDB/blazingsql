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
#include <spdlog/sinks/rotating_file_sink.h>

#include <algorithm>
#include <cuda_runtime.h>
#include <memory>
#include <chrono>
#include <thread>         // std::this_thread::sleep_for
#include <fstream>
#include <utility>
#include <memory>

#include <transport/io/reader_writer.h>

#include <blazingdb/io/Config/BlazingContext.h>
#include <blazingdb/io/Library/Logging/CoutOutput.h>
#include <blazingdb/io/Library/Logging/Logger.h>
#include "blazingdb/io/Library/Logging/ServiceLogging.h"
#include <blazingdb/io/Util/StringUtil.h>

#include "communication/CommunicationData.h"

#include <bmr/initializer.h>
#include <bmr/BlazingMemoryResource.h>

#include "error.hpp"

#include "cudf/detail/gather.hpp"
#include "communication/CommunicationInterface/node.hpp"
#include "communication/CommunicationInterface/protocols.hpp"
#include "communication/CommunicationInterface/messageSender.hpp"
#include "communication/CommunicationInterface/messageListener.hpp"
#include "execution_graph/logic_controllers/taskflow/kernel.h"
#include "execution_graph/logic_controllers/taskflow/executor.h"

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
	uint16_t ralId, std::string flush_level,
	std::string logger_level_wanted,
	std::size_t max_size_logging,
	bool simple_log=true) {

	std::shared_ptr<spdlog::logger> existing_logger = spdlog::get(loggingName);
	if (existing_logger){ // if logger already exists, dont initialize again
		return;
	}

	auto stdout_sink = std::make_shared<spdlog::sinks::stdout_color_sink_mt>();
	stdout_sink->set_pattern("[%T.%e] [%^%l%$] %v");
	stdout_sink->set_level(spdlog::level::err);

	// TODO: discuss how we should handle this
	// if max_num_files = 4 -> will have: RAL.0.log, RAL.0.1.log, RAL.0.2.log, RAL.0.3.log, RAL.0.4.log
	auto max_num_files = 0;
	auto rotating_sink = std::make_shared<spdlog::sinks::rotating_file_sink_mt> (fileName, max_size_logging, max_num_files);
	if (simple_log) {
		rotating_sink->set_pattern(fmt::format("%v"));
	} else {
		rotating_sink->set_pattern(fmt::format("%Y-%m-%d %T.%e|{}|%^%l%$|%v", ralId));
	}

	// We want ALL levels of info to be registered. So using by default `trace` level
	rotating_sink->set_level(spdlog::level::trace);
	spdlog::sinks_init_list sink_list = {stdout_sink, rotating_sink};
	auto logger = std::make_shared<spdlog::async_logger>(loggingName, sink_list, spdlog::thread_pool(), spdlog::async_overflow_policy::block);

	// level of logs
	logger->set_level(log_level_str_to_enum(logger_level_wanted));

	spdlog::register_logger(logger);

	spdlog::flush_on(log_level_str_to_enum(flush_level));
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
  ~AddressExchangerForSender() {
		closeCurrentConnection();
		CheckError(close(lsock_), "close sender");
	}

  AddressExchangerForSender(const std::uint16_t port) {
    struct sockaddr_in inaddr;

    lsock_ = -1;
    dsock_ = -1;
    int optval = 1;
    int ret;

    lsock_ = socket(AF_INET, SOCK_STREAM, 0);
    if (lsock_ < 0) {
      std::cerr << "open server socket" << std::endl;
      throw std::runtime_error("open server socket");
    }

    optval = 1;
    ret = setsockopt(lsock_, SOL_SOCKET, SO_REUSEADDR, &optval, sizeof(optval));
    if (ret < 0) {
      std::cerr << "server setsockopt()" << std::endl;
      close(lsock_);
      throw std::runtime_error("server setsockopt()");
    }

    inaddr.sin_family = AF_INET;
    inaddr.sin_port = htons(port);
    inaddr.sin_addr.s_addr = INADDR_ANY;
    std::memset(inaddr.sin_zero, 0, sizeof(inaddr.sin_zero));
    ret = bind(lsock_, (struct sockaddr *) &inaddr, sizeof(inaddr));
    if (ret < 0) {
      close(lsock_);
      throw std::runtime_error("bind server");
    }

    ret = listen(lsock_, SOMAXCONN);
    if (ret < 0) {
      close(lsock_);
      throw std::runtime_error("listen server");
    }


    // dsock_ = accept(lsock_, NULL, NULL);
    // if (dsock_ < 0) {
    //   std::cout << "accept server" << std::endl;
    //   close(lsock_);
    //   throw std::runtime_error("accept server");
    // }

    // close(lsock_);

    // CheckError(dsock_ < 0, "server_connect");
  }

	char *get_ip_str(const struct sockaddr *sa, char *s, size_t maxlen)
	{
			switch(sa->sa_family) {
					case AF_INET:
							inet_ntop(AF_INET, &(((struct sockaddr_in *)sa)->sin_addr),
											s, maxlen);
							break;

					case AF_INET6:
							inet_ntop(AF_INET6, &(((struct sockaddr_in6 *)sa)->sin6_addr),
											s, maxlen);
							break;

					default:
							strncpy(s, "Unknown AF", maxlen);
							return NULL;
			}

			return s;
	}

	bool acceptConnection() {
		struct sockaddr address;
    unsigned int addrlen = sizeof(address);
    dsock_ = accept(lsock_, &address, (socklen_t*)&addrlen);
    if (dsock_ < 0) {
      close(lsock_);
      throw std::runtime_error("accept server");
    }

    CheckError(dsock_ < 0, "server_connect");

		char str_buffer[INET6_ADDRSTRLEN];
		get_ip_str(&address, str_buffer, INET6_ADDRSTRLEN);

		return true;
	}

	void closeCurrentConnection(){
		if (dsock_ != -1) {
			CheckError(close(dsock_), "close sender fd");
			dsock_ = -1;
		}
	}

  int fd() final { return dsock_; }

private:
  int dsock_;
	int lsock_;
};

class AddressExchangerForReceiver : public AddressExchanger {
public:
  ~AddressExchangerForReceiver() {
		CheckError(close(fd()), "close receiver");
	}

  AddressExchangerForReceiver(const std::uint16_t port, const char *hostname) {
    struct sockaddr_in conn_addr;
    struct hostent *he;
    int connfd;
    int ret;

    connfd = socket(AF_INET, SOCK_STREAM, 0);
    if (connfd < 0) {
      const std::string message = "open client socket";
      throw std::runtime_error(message);
    }

    he = gethostbyname(hostname);
    if (he == NULL || he->h_addr_list == NULL) {
      const std::string message = "found a host";
      close(connfd);
      throw std::runtime_error(message);
    }

    conn_addr.sin_family = he->h_addrtype;
    conn_addr.sin_port = htons(port);

    std::memcpy(&conn_addr.sin_addr, he->h_addr_list[0], he->h_length);
    std::memset(conn_addr.sin_zero, 0, sizeof(conn_addr.sin_zero));


    int num_attempts = 50;
    int attempt = 0;
    while (attempt < num_attempts){
        ret = connect(connfd, (struct sockaddr *) &conn_addr, sizeof(conn_addr));
        if (ret < 0) {
            attempt++;
		std::this_thread::sleep_for (std::chrono::seconds(1));
        } else {
		break;
	}
	if (attempt == num_attempts){
	      const std::string message = "could not connect to client";
	      close(connfd);
	      throw std::runtime_error(message);
	}
    }

    CheckError(connfd < 0, "server_connect");
    connfd_ = connfd;
  }

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



std::mutex initialize_lock;
bool initialized = false;

//=============================================================================

void printLoggerHeader(){
    std::shared_ptr<spdlog::logger> queries_logger = spdlog::get("queries_logger");
    if(queries_logger){
        queries_logger->info("ral_id|query_id|start_time|plan|query");
    }

    std::shared_ptr<spdlog::logger> kernels_logger = spdlog::get("kernels_logger");
    if(kernels_logger){
        kernels_logger->info("ral_id|query_id|kernel_id|is_kernel|kernel_type");
    }

    std::shared_ptr<spdlog::logger> kernels_edges_logger = spdlog::get("kernels_edges_logger");
    if(kernels_edges_logger){
        kernels_edges_logger->info("ral_id|query_id|source|sink");
    }

    std::shared_ptr<spdlog::logger> events_logger = spdlog::get("events_logger");
    if(events_logger){
        events_logger->info("ral_id|query_id|kernel_id|input_num_rows|input_num_bytes|output_num_rows|output_num_bytes|event_type|timestamp_begin|timestamp_end");
    }

    std::shared_ptr<spdlog::logger> cache_events_logger = spdlog::get("cache_events_logger");
    if(cache_events_logger){
        cache_events_logger->info("ral_id|query_id|source|sink|num_rows|num_bytes|event_type|timestamp_begin|timestamp_end");
    }

    std::shared_ptr<spdlog::logger> batch_logger = spdlog::get("batch_logger");
    if(batch_logger){
        batch_logger->info("log_time|node_id|type|query_id|step|substep|info|duration|extra1|data1|extra2|data2");
    }

    std::shared_ptr<spdlog::logger> input_comms = spdlog::get("input_comms");
    if(input_comms){
        input_comms->info("unique_id|ral_id|query_id|kernel_id|dest_ral_id|dest_ral_count|dest_cache_id|message_id|phase");
    }

    std::shared_ptr<spdlog::logger> output_comms = spdlog::get("output_comms");
    if(output_comms){
        output_comms->info("unique_id|ral_id|query_id|kernel_id|dest_ral_id|dest_ral_count|dest_cache_id|message_id|phase");
    }
}


/**
* Initializes the engine and gives us shared pointers to both our transport out cache
* and the cache we use for receiving messages
*
*/
std::pair<std::pair<std::shared_ptr<CacheMachine>,std::shared_ptr<CacheMachine> >, int> initialize(uint16_t ralId,
	std::string worker_id,
	std::string network_iface_name,
	int ralCommunicationPort,
	std::vector<NodeMetaDataUCP> workers_ucp_info,
	bool singleNode,
	std::map<std::string, std::string> config_options,
	std::string allocation_mode,
	std::size_t initial_pool_size,
	std::size_t maximum_pool_size,
	bool enable_logging) {

	std::lock_guard<std::mutex> init_lock(initialize_lock);

	float device_mem_resouce_consumption_thresh = 0.6;
	auto config_it = config_options.find("BLAZING_DEVICE_MEM_CONSUMPTION_THRESHOLD");
	if (config_it != config_options.end()){
		device_mem_resouce_consumption_thresh = std::stof(config_options["BLAZING_DEVICE_MEM_CONSUMPTION_THRESHOLD"]);
	}
	std::string logging_dir = "blazing_log";
	config_it = config_options.find("BLAZING_LOGGING_DIRECTORY");
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

	std::string allocator_logging_file = "";
	if (enable_logging && !logging_directory_missing){
		allocator_logging_file = logging_dir + "/allocator." + std::to_string(ralId) + ".log";
	}
	BlazingRMMInitialize(allocation_mode, initial_pool_size, maximum_pool_size, allocator_logging_file, device_mem_resouce_consumption_thresh);

	// ---------------------------------------------------------------------------
	// DISCLAIMER
	// TODO: Support proper locale support for non-US cases (percy)
	std::setlocale(LC_ALL, "en_US.UTF-8");
	std::setlocale(LC_NUMERIC, "en_US.UTF-8");
	// ---------------------------------------------------------------------------

	//signal(SIGSEGV, handler);   // install our handler. This is for debugging.

	std::string ralHost = get_ip(network_iface_name);

	std::string initLogMsg = "INITIALIZING RAL. RAL ID: " + std::to_string(ralId)  + ", ";
	initLogMsg = initLogMsg + "RAL Host: " + ralHost + ":" + std::to_string(ralCommunicationPort) + ", ";
	initLogMsg = initLogMsg + "Network Interface: " + network_iface_name + ", ";
	initLogMsg = initLogMsg + (singleNode ? ", Is Single Node, " : ", Is Not Single Node, ");

	const char * env_cuda_device = std::getenv("CUDA_VISIBLE_DEVICES");
	std::string env_cuda_device_str = env_cuda_device == nullptr ? "" : std::string(env_cuda_device);
	initLogMsg = initLogMsg + "CUDA_VISIBLE_DEVICES is set to: " + env_cuda_device_str + ", ";

	size_t buffers_size = 1048576;  // 10 MBs
	auto iter = config_options.find("TRANSPORT_BUFFER_BYTE_SIZE");
	if (iter != config_options.end()){
		buffers_size = std::stoi(config_options["TRANSPORT_BUFFER_BYTE_SIZE"]);
	}
	int num_comm_threads = 20;
	iter = config_options.find("MAX_SEND_MESSAGE_THREADS");
	if (iter != config_options.end()){
		num_comm_threads = std::stoi(config_options["MAX_SEND_MESSAGE_THREADS"]);
	}
	int num_buffers = 100;
	iter = config_options.find("TRANSPORT_POOL_NUM_BUFFERS");
	if (iter != config_options.end()){
		num_buffers = std::stoi(config_options["TRANSPORT_POOL_NUM_BUFFERS"]);
	}
	blazingdb::transport::io::setPinnedBufferProvider(buffers_size, num_buffers);

	//to avoid redundancy the default value or user defined value for this parameter is placed on the pyblazing side
	assert( config_options.find("BLAZ_HOST_MEM_CONSUMPTION_THRESHOLD") != config_options.end() );
	float host_memory_quota = std::stof(config_options["BLAZ_HOST_MEM_CONSUMPTION_THRESHOLD"]);

	blazing_host_memory_resource::getInstance().initialize(host_memory_quota);


	// Init AWS S3 ... TODO see if we need to call shutdown and avoid leaks from s3 percy
	BlazingContext::getInstance()->initExternalSystems();

	int executor_threads = 10;
	auto exec_it = config_options.find("EXECUTOR_THREADS");
	if (exec_it != config_options.end()){
		executor_threads = std::stoi(config_options["EXECUTOR_THREADS"]);
	}
	
	std::string flush_level = "warn";
	
	auto log_it = config_options.find("LOGGING_FLUSH_LEVEL");
	if (log_it != config_options.end()){
		flush_level = config_options["LOGGING_FLUSH_LEVEL"];
	}

	std::string enable_general_engine_logs;
    log_it = config_options.find("ENABLE_GENERAL_ENGINE_LOGS");
    if (log_it != config_options.end()){
        enable_general_engine_logs = config_options["ENABLE_GENERAL_ENGINE_LOGS"];
    }

    std::string enable_comms_logs;
    log_it = config_options.find("ENABLE_COMMS_LOGS");
    if (log_it != config_options.end()){
        enable_comms_logs = config_options["ENABLE_COMMS_LOGS"];
    }

    std::string enable_caches_logs;
    log_it = config_options.find("ENABLE_CACHES_LOGS");
    if (log_it != config_options.end()){
        enable_caches_logs = config_options["ENABLE_CACHES_LOGS"];
    }

    std::string enable_other_engine_logs;
    log_it = config_options.find("ENABLE_OTHER_ENGINE_LOGS");
    if (log_it != config_options.end()){
        enable_other_engine_logs = config_options["ENABLE_OTHER_ENGINE_LOGS"];
    }

	std::string logger_level_wanted = "trace";
	auto log_level_it = config_options.find("LOGGING_LEVEL");
	if (log_level_it != config_options.end()){
		logger_level_wanted = config_options["LOGGING_LEVEL"];
	}

	std::size_t max_size_logging = 1073741824; // 1 GB
	auto size_log_it = config_options.find("LOGGING_MAX_SIZE_PER_FILE");
	if (size_log_it != config_options.end()){
		max_size_logging = std::stoi(config_options["LOGGING_MAX_SIZE_PER_FILE"]);
	}

	if (!initialized){

		// spdlog batch logger
		spdlog::shutdown();

		spdlog::init_thread_pool(8192, 1);

		if(enable_general_engine_logs=="True") {
		    std::string batchLoggerFileName = logging_dir + "/RAL." + std::to_string(ralId) + ".log";
		    create_logger(batchLoggerFileName, "batch_logger", ralId, flush_level, logger_level_wanted, max_size_logging, false);
        }

        if(enable_comms_logs=="True"){
            std::string outputCommunicationLoggerFileName = logging_dir + "/output_comms." + std::to_string(ralId) + ".log";
            create_logger(outputCommunicationLoggerFileName, "output_comms", ralId, flush_level, logger_level_wanted, max_size_logging);

            std::string inputCommunicationLoggerFileName = logging_dir + "/input_comms." + std::to_string(ralId) + ".log";
            create_logger(inputCommunicationLoggerFileName, "input_comms", ralId, flush_level, logger_level_wanted, max_size_logging);
        }

        if(enable_other_engine_logs=="True"){
            std::string queriesFileName = logging_dir + "/bsql_queries." + std::to_string(ralId) + ".log";
            create_logger(queriesFileName, "queries_logger", ralId, flush_level, logger_level_wanted, max_size_logging);

            std::string kernelsFileName = logging_dir + "/bsql_kernels." + std::to_string(ralId) + ".log";
            create_logger(kernelsFileName, "kernels_logger", ralId, flush_level, logger_level_wanted, max_size_logging);

            std::string kernelsEdgesFileName = logging_dir + "/bsql_kernels_edges." + std::to_string(ralId) + ".log";
            create_logger(kernelsEdgesFileName, "kernels_edges_logger", ralId, flush_level, logger_level_wanted, max_size_logging);

            std::string kernelEventsFileName = logging_dir + "/bsql_kernel_events." + std::to_string(ralId) + ".log";
            create_logger(kernelEventsFileName, "events_logger", ralId, flush_level, logger_level_wanted, max_size_logging);
        }

        if(enable_caches_logs=="True"){
            std::string cacheEventsFileName = logging_dir + "/bsql_cache_events." + std::to_string(ralId) + ".log";
            create_logger(cacheEventsFileName, "cache_events_logger", ralId, flush_level, logger_level_wanted, max_size_logging);
        }

        printLoggerHeader();
	} 

	std::shared_ptr<spdlog::logger> logger = spdlog::get("batch_logger");
	if (logging_directory_missing){
	    if(logger){
		    logger->error("|||{info}|||||","info"_a="BLAZING_LOGGING_DIRECTORY not found. It was not created.");
	    }
	}

	if(logger){
	    logger->debug("|||{info}|||||","info"_a=initLogMsg);
	}
	std::map<std::string, std::string> product_details = getProductDetails();
	std::string product_details_str = "Product Details: ";
	std::map<std::string, std::string>::iterator it = product_details.begin();
	while (it != product_details.end())	{
		product_details_str += it->first + ": " + it->second + "; ";
		it++;
	}
	if(logger){
	    logger->debug("|||{info}|||||","info"_a=product_details_str);
	}


	blazing_device_memory_resource* resource = &blazing_device_memory_resource::getInstance();
	std::string alloc_info = "allocation_mode: " + allocation_mode;
	alloc_info += ", total_memory: " + std::to_string(resource->get_total_memory());
	alloc_info += ", memory_limit: " + std::to_string(resource->get_memory_limit());
	alloc_info += ", type: " + resource->get_type();
	alloc_info += ", initial_pool_size: " + std::to_string(initial_pool_size);
	alloc_info += ", maximum_pool_size: " + std::to_string(maximum_pool_size);
	alloc_info += ", allocator_logging_file: " + allocator_logging_file;
	if(logger){
	    logger->debug("|||{info}|||||","info"_a=alloc_info);
	}


	std::string orc_files_path;
	iter = config_options.find("BLAZING_CACHE_DIRECTORY");
	if (iter != config_options.end()) {
		orc_files_path = config_options["BLAZING_CACHE_DIRECTORY"];
	}
	if (!singleNode) {
		orc_files_path += std::to_string(ralId);
	}

	auto & communicationData = ral::communication::CommunicationData::getInstance();
	communicationData.initialize(worker_id, orc_files_path);

	auto output_input_caches = std::make_pair(std::make_shared<CacheMachine>(nullptr, "messages_out", false,CACHE_LEVEL_CPU ),std::make_shared<CacheMachine>(nullptr, "messages_in", false));

	// start ucp servers
	if(!singleNode){
		std::map<std::string, comm::node> nodes_info_map;

		comm::blazing_protocol protocol = comm::blazing_protocol::tcp;
		std::string protocol_value = StringUtil::toLower(config_options["PROTOCOL"]);
		if (protocol_value == "ucx"){
			protocol = comm::blazing_protocol::ucx;
		}
		ucp_context_h ucp_context = nullptr;
		ucp_worker_h self_worker = nullptr;
		if(protocol == comm::blazing_protocol::ucx){
			ucp_context = reinterpret_cast<ucp_context_h>(workers_ucp_info[0].context_handle);

			self_worker = CreatetUcpWorker(ucp_context);

			UcpWorkerAddress ucpWorkerAddress = GetUcpWorkerAddress(self_worker);

			std::map<std::string, UcpWorkerAddress> peer_addresses_map;
			auto th = std::thread([ralCommunicationPort, total_peers=workers_ucp_info.size(), &peer_addresses_map, worker_id, workers_ucp_info](){
				AddressExchangerForSender exchanger(ralCommunicationPort);
				for (size_t i = 0; i < total_peers; i++){
					if(workers_ucp_info[i].worker_id == worker_id){
						continue;
					}
					if (exchanger.acceptConnection()){
						int ret;

						// Receive worker_id size
						size_t worker_id_buff_size;
						ret = recv(exchanger.fd(), &worker_id_buff_size, sizeof(size_t), MSG_WAITALL);
						CheckError(static_cast<size_t>(ret) != sizeof(size_t), "recv worker_id_buff_size");

						// Receive worker_id
						std::string worker_id(worker_id_buff_size, '\0');
						ret = recv(exchanger.fd(), &worker_id[0], worker_id.size(), MSG_WAITALL);
						CheckError(static_cast<size_t>(ret) != worker_id.size(), "recv worker_id");

						// Receive ucp_worker_address size
						size_t ucp_worker_address_size;
						ret = recv(exchanger.fd(), &ucp_worker_address_size, sizeof(size_t), MSG_WAITALL);
						CheckError(static_cast<size_t>(ret) != sizeof(size_t), "recv ucp_worker_address_size");

						// Receive ucp_worker_address
						std::uint8_t *data = new std::uint8_t[ucp_worker_address_size];
						UcpWorkerAddress peerUcpWorkerAddress{
								reinterpret_cast<ucp_address_t *>(data),
								ucp_worker_address_size};

						ret = recv(exchanger.fd(), peerUcpWorkerAddress.address, ucp_worker_address_size, MSG_WAITALL);
						CheckError(static_cast<size_t>(ret) != ucp_worker_address_size, "recv ucp_worker_address");

						peer_addresses_map.emplace(worker_id, peerUcpWorkerAddress);

						exchanger.closeCurrentConnection();
					}
				}
			});

			std::this_thread::sleep_for(std::chrono::seconds(1));
			for (auto &&worker_info : workers_ucp_info){
				if(worker_info.worker_id == worker_id){
					continue;
				}
				AddressExchangerForReceiver exchanger(worker_info.port, worker_info.ip.c_str());
				int ret;

				// Send worker_id size
				size_t worker_id_buff_size = worker_id.size();
				ret = send(exchanger.fd(), &worker_id_buff_size, sizeof(size_t), 0);
				CheckError(static_cast<size_t>(ret) != sizeof(size_t), "send worker_id_buff_size");

				// Send worker_id
				ret = send(exchanger.fd(), worker_id.data(), worker_id.size(), 0);
				CheckError(static_cast<size_t>(ret) != worker_id.size(), "send worker_id");

				// Send ucp_worker_address size
				ret = send(exchanger.fd(), &ucpWorkerAddress.length, sizeof(size_t), 0);
				CheckError(static_cast<size_t>(ret) != sizeof(size_t), "send ucp_worker_address_size");

				// Send ucp_worker_address
				ret = send(exchanger.fd(), ucpWorkerAddress.address, ucpWorkerAddress.length, 0);
				CheckError(static_cast<size_t>(ret) != ucpWorkerAddress.length, "send ucp_worker_address");
			}

			th.join();

			for (auto &&worker_info : workers_ucp_info){
				if(worker_info.worker_id == worker_id){
					continue;
				}
				UcpWorkerAddress peerUcpWorkerAddress = peer_addresses_map[worker_info.worker_id];
				ucp_ep_h ucp_ep = CreateUcpEp(self_worker, peerUcpWorkerAddress);

				// std::cout << '[' << std::hex << std::this_thread::get_id()
				// 					<< "] local: " << std::hex
				// 					<< *reinterpret_cast<std::size_t *>(ucpWorkerAddress.address) << ' '
				// 					<< ucpWorkerAddress.length << std::endl
				// 					<< '[' << std::hex << std::this_thread::get_id()
				// 					<< "] peer: " << std::hex
				// 					<< *reinterpret_cast<std::size_t *>(peerUcpWorkerAddress.address)
				// 					<<' ' << peerUcpWorkerAddress.length << std::endl;

				// auto worker_info = workers_ucp_info[0];
				nodes_info_map.emplace(worker_info.worker_id, comm::node(ralId, worker_info.worker_id, ucp_ep, self_worker));
			}

			comm::ucx_message_listener::initialize_message_listener(
				ucp_context, self_worker,nodes_info_map,20);
			comm::ucx_message_listener::get_instance()->poll_begin_message_tag(true);

		}else{


			for (auto &&worker_info : workers_ucp_info){
				if(worker_info.worker_id == worker_id){
					continue;
				}

				nodes_info_map.emplace(worker_info.worker_id, comm::node(ralId, worker_info.worker_id, worker_info.ip, worker_info.port));
			}

			comm::tcp_message_listener::initialize_message_listener(nodes_info_map,ralCommunicationPort,num_comm_threads);
			comm::tcp_message_listener::get_instance()->start_polling();
			ralCommunicationPort = comm::tcp_message_listener::get_instance()->get_port(); // if the listener was already initialized, we want to get the port that was originally set and send that back to python side
		}
		comm::message_sender::initialize_instance(output_input_caches.first, output_input_caches.second,
			nodes_info_map,
			num_comm_threads, ucp_context, self_worker, ralId,protocol);
		comm::message_sender::get_instance()->run_polling();

		output_input_caches.first = comm::message_sender::get_instance()->get_output_cache();
		output_input_caches.second = comm::message_sender::get_instance()->get_input_cache();
	}

	double processing_memory_limit_threshold = 0.9;
	config_it = config_options.find("BLAZING_PROCESSING_DEVICE_MEM_CONSUMPTION_THRESHOLD");
	if (config_it != config_options.end()){
		processing_memory_limit_threshold = std::stod(config_options["BLAZING_PROCESSING_DEVICE_MEM_CONSUMPTION_THRESHOLD"]);
	}

	ral::execution::executor::init_executor(executor_threads, processing_memory_limit_threshold);
	initialized = true;
	return std::make_pair(output_input_caches, ralCommunicationPort);	
}

void finalize() {

	BlazingRMMFinalize();
	spdlog::shutdown();
	cudaDeviceReset();
	exit(0);
}

error_code_t initialize_C(uint16_t ralId,
	std::string worker_id,
	std::string network_iface_name,

	int ralCommunicationPort,
	std::vector<NodeMetaDataUCP> workers_ucp_info,
	bool singleNode,
	std::map<std::string, std::string> config_options,
	std::string allocation_mode,
	std::size_t initial_pool_size,
	std::size_t maximum_pool_size,
	bool enable_logging) {

	try {
		initialize(ralId,
			worker_id,
			network_iface_name,
			ralCommunicationPort,
			workers_ucp_info,
			singleNode,
			config_options,
			allocation_mode,
			initial_pool_size,
			maximum_pool_size,
			enable_logging);
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

size_t getFreeMemory() {
	BlazingMemoryResource* resource = &blazing_device_memory_resource::getInstance();
	size_t total_free_memory = resource->get_memory_limit() - resource->get_memory_used();
	return total_free_memory;
}

void resetMaxMemoryUsed(int to) {
	blazing_device_memory_resource* resource = &blazing_device_memory_resource::getInstance();
	resource->reset_max_memory_used(to);
}

size_t getMaxMemoryUsed() {
    blazing_device_memory_resource* resource = &blazing_device_memory_resource::getInstance();
	return resource->get_max_memory_used();
}
