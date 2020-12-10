#include <algorithm>
#include <cstring>
#include <iostream>
#include <thread>

#include <netdb.h>
#include <unistd.h>

#include <ucp/api/ucp.h>
#include <ucs/memory/memory_type.h>

#ifdef USE_CUDA
#include <cuda.h>
#include <cuda_runtime.h>

static inline void CheckCudaCall(cudaError_t cudaError) {
  if (cudaSuccess != cudaError) {
    std::cerr << "Cuda call failed: " << cudaGetErrorString(cudaError)
              << std::endl;
    throw std::runtime_error("Cuda call failed");
  }
}
#endif

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

//- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

const std::uint16_t exchangingPort = 9876;

//- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

class AppContext {
public:
  bool completed;
};

static void request_init(void *request) {
  // std::cout << "Request init" << std::endl;
  AppContext *c = reinterpret_cast<AppContext *>(request);
  c->completed = 0;
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
  ucp_params.request_size = sizeof(AppContext);
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
  worker_params.thread_mode = UCS_THREAD_MODE_MULTI;

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
  CheckError(status != UCS_OK, "ucp_worker_get_address", [&ucp_worker]() {
    ucp_worker_destroy(ucp_worker);
  });

  return ucpWorkerAddress;
}

//- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

template <class Callback>
void Run(Callback &&callback,
         std::unique_ptr<AddressExchanger> &&addressExchanger) {
  ucp_context_h ucp_context = CreateUcpContext();
  ucp_worker_h ucp_worker = CreatetUcpWorker(ucp_context);
  UcpWorkerAddress ucpWorkerAddress = GetUcpWorkerAddress(ucp_worker);

  UcpWorkerAddress peerUcpWorkerAddress =
      addressExchanger->Exchange(ucpWorkerAddress);

  std::cout << '[' << std::hex << std::this_thread::get_id()
            << "] local: " << std::hex
            << *reinterpret_cast<std::size_t *>(ucpWorkerAddress.address) << ' '
            << ucpWorkerAddress.length << std::endl
            << '[' << std::hex << std::this_thread::get_id()
            << "] peer: " << std::hex
            << *reinterpret_cast<std::size_t *>(peerUcpWorkerAddress.address)
            << ' ' << peerUcpWorkerAddress.length << std::endl;

  ucp_context_attr_t attr;
  attr.field_mask = UCP_ATTR_FIELD_REQUEST_SIZE;

  ucs_status_t status = ucp_context_query(ucp_context, &attr);
  CheckError(status != UCS_OK, "ucp_context_query");

  static const std::size_t testStringLength = std::pow(10, 5);
  callback(
      peerUcpWorkerAddress, ucp_worker, attr.request_size, testStringLength);

  ucp_worker_release_address(ucp_worker, ucpWorkerAddress.address);
  ucp_worker_destroy(ucp_worker);
  ucp_cleanup(ucp_context);
}

static class ErrorHandling {
public:
  ucp_err_handling_mode_t ucp_err_mode;
  int failure;
} err_handling_opt;

static void failure_handler(void *arg, ucp_ep_h, ucs_status_t status) {
  ucs_status_t *arg_status = static_cast<ucs_status_t *>(arg);
  std::cout << '[' << std::hex << std::this_thread::get_id()
            << "] failure handler called with status " << status << " ("
            << ucs_status_string(status) << ')' << std::endl;
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

class Message {
public:
  std::uint64_t data_len;
};

static ucs_memory_type_t test_mem_type =
#ifdef USE_CUDA
    UCS_MEMORY_TYPE_CUDA;
#else
    UCS_MEMORY_TYPE_HOST;
#endif


static inline void *mem_type_malloc(std::size_t length) {
  void *ptr;

  switch (test_mem_type) {
  case UCS_MEMORY_TYPE_HOST: ptr = std::malloc(length); break;
#ifdef USE_CUDA
  case UCS_MEMORY_TYPE_CUDA: CheckCudaCall(cudaMalloc(&ptr, length)); break;
  case UCS_MEMORY_TYPE_CUDA_MANAGED:
    CheckCudaCall(cudaMallocManaged(&ptr, length, cudaMemAttachGlobal));
    break;
#endif
  default:
    std::cerr << "Unsupported memory type: " << test_mem_type << std::endl;
    throw std::runtime_error("Unsupported memory type");
  }

  CheckError(ptr == nullptr, "Allocate memory");

  return ptr;
}

static inline void *mem_type_memset(void *dst, int value, std::size_t count) {
  switch (test_mem_type) {
  case UCS_MEMORY_TYPE_HOST: std::memset(dst, value, count); break;
#ifdef USE_CUDA
  case UCS_MEMORY_TYPE_CUDA:
  case UCS_MEMORY_TYPE_CUDA_MANAGED:
    CheckCudaCall(cudaMemset(dst, value, count));
    break;
#endif
  default:
    std::cerr << "Unsupported memory type: " << test_mem_type << std::endl;
    throw std::runtime_error("Unsupported memory type");
  }

  return dst;
}

static inline void *
mem_type_memcpy(void *dst, const void *src, std::size_t count) {
  switch (test_mem_type) {
  case UCS_MEMORY_TYPE_HOST: std::memcpy(dst, src, count); break;
#ifdef USE_CUDA
  case UCS_MEMORY_TYPE_CUDA:
  case UCS_MEMORY_TYPE_CUDA_MANAGED:
    CheckCudaCall(cudaMemcpy(dst, src, count, cudaMemcpyDefault));
    break;
#endif
  default:
    std::cerr << "Unsupported memory type: " << test_mem_type << std::endl;
    throw std::runtime_error("Unsupported memory type");
  }

  return dst;
}

static inline void mem_type_free(void *address) {
  switch (test_mem_type) {
  case UCS_MEMORY_TYPE_HOST: free(address); break;
#ifdef USE_CUDA
  case UCS_MEMORY_TYPE_CUDA:
  case UCS_MEMORY_TYPE_CUDA_MANAGED: CheckCudaCall(cudaFree(address)); break;
#endif
  default:
    std::cerr << "Unsupported memory type: " << test_mem_type << std::endl;
    throw std::runtime_error("Unsupported memory type");
  }
}

static inline void set_msg_data_len(Message *message, std::uint64_t data_len) {
  mem_type_memcpy(&message->data_len, &data_len, sizeof(data_len));
}

static inline void generate_test_string(char *str, const std::size_t size) {
  char *tmp = reinterpret_cast<char *>(std::calloc(1, size));
  CheckError(tmp == nullptr, "allocate memory");

  std::generate_n(
      tmp, size - 1, [n = -1]() mutable { return 'A' + (++n % 26); });
  mem_type_memcpy(str, tmp, size);

  std::free(tmp);
}

static const ucp_tag_t tag_base = 0x1337a880u;
static const ucp_tag_t tag_mask = std::numeric_limits<std::uint64_t>::max();

class Package {
public:
  Message *message() noexcept { return message_; }

  std::size_t length() noexcept { return length_; }

  static inline Package MakeTestString(const std::size_t stringLength) {
    const std::size_t length = sizeof(Message) + stringLength;
    Message *message = reinterpret_cast<Message *>(mem_type_malloc(length));
    mem_type_memset(message, 0, length);
    set_msg_data_len(message, length - sizeof(*message));
    try {
      generate_test_string(reinterpret_cast<char *>(message + 1), stringLength);
    } catch (std::exception) {
      std::cerr << "generate test string" << std::endl;
      mem_type_free(message);
      throw std::runtime_error("generate test string");
    }
    return Package{message, length};
  }

  static inline Package MakeToStore(const std::size_t stringLength) {
    const std::size_t length = sizeof(Message) + stringLength;
    Message *message = reinterpret_cast<Message *>(mem_type_malloc(length));
    mem_type_memset(message, 0, length);
    return Package{message, length};
  }

private:
  explicit Package(Message *message, std::size_t length)
      : message_{message}, length_{length} {}

  friend std::ostream &operator<<(std::ostream &os, Package &package) {
    const std::size_t length = package.length() - sizeof(*package.message());
    char *str = reinterpret_cast<char *>(std::calloc(1, length));
    if (str != nullptr) {
      //mem_type_memcpy(str, package.message() + 1, length);
      //os << str;
      os << std::dec << length;
      std::free(str);
    } else {
      throw std::runtime_error("Memory allocation failed");
    }
    return os;
  }

  Message *message_;
  std::size_t length_;
};

template <class Callable>
std::vector<Package> MakePackages(const std::size_t length,
                                  const std::size_t stringLength,
                                  Callable &&callable) {
  std::vector<Package> packages;
  packages.reserve(length);
  std::generate_n(std::back_inserter(packages),
                  length,
                  [n = stringLength, &callable]() mutable {
                    return std::forward<Callable>(callable)(++n);
                  });
  return packages;
}

std::vector<Package> MakePackagesForSending(const std::size_t length,
                                            const std::size_t stringLength) {
  return MakePackages(length, stringLength, Package::MakeTestString);
}

std::vector<Package> MakePackagesForReceiving(const std::size_t length,
                                              const std::size_t stringLength) {
  return MakePackages(length, stringLength, Package::MakeToStore);
}

//- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

void Send(Package &package,
          ucp_ep_h ucp_ep,
          ucp_worker_h ucp_worker,
          ucp_tag_t tag,
          const std::size_t requestSize) {
  char *request = reinterpret_cast<char *>(std::malloc(requestSize));
  ucs_status_t status = ucp_tag_send_nbr(ucp_ep,
                                         package.message(),
                                         package.length(),
                                         ucp_dt_make_contig(1),
                                         tag,
                                         request + requestSize);
  if (status != UCS_INPROGRESS) { return; }

  do {
    ucp_worker_progress(ucp_worker);
    status = ucp_request_check_status(request + requestSize);
  } while (status == UCS_INPROGRESS);
}

Package
Receive(ucp_worker_h ucp_worker, ucp_tag_t tag, const std::size_t requestSize) {
  ucp_tag_message_h tag_message;
  ucp_tag_recv_info_t recv_info;

  do {
    ucp_worker_progress(ucp_worker);
    tag_message = ucp_tag_probe_nb(ucp_worker, tag, tag_mask, 0, &recv_info);
  } while (tag_message == nullptr);

  Package package = Package::MakeToStore(recv_info.length - sizeof(Message));

  char *request = reinterpret_cast<char *>(std::malloc(requestSize));
  ucs_status_t status = ucp_tag_recv_nbr(ucp_worker,
                                         package.message(),
                                         package.length(),
                                         ucp_dt_make_contig(1),
                                         tag,
                                         tag_mask,
                                         request + requestSize);

  if (UCS_STATUS_IS_ERR(status)) {
    std::cerr << "ucp_tag_recv_nb returned status "
              << ucs_status_string(status);
    throw std::runtime_error("ucp_tag_recv_nb returned status");
  }

  do {
    ucp_worker_progress(ucp_worker);
    ucp_tag_recv_info_t info_tag;
    status = ucp_tag_recv_request_test(request + requestSize, &info_tag);
  } while (status == UCS_INPROGRESS);

  return package;
}

static const std::size_t packagesLength = 10;

void SenderCall(const UcpWorkerAddress &peerUcpWorkerAddress,
                ucp_worker_h ucp_worker,
                const std::size_t requestSize,
                const std::size_t testStringLength) {
  ucp_ep_h ucp_ep = CreateUcpEp(ucp_worker, peerUcpWorkerAddress);

  std::vector<Package> packages =
      MakePackagesForSending(packagesLength, testStringLength);

  ucp_tag_t tag = tag_base;
  for (Package &package : packages) {
    std::thread([&package, tag, ucp_ep, ucp_worker, requestSize]() {
      Send(package, ucp_ep, ucp_worker, tag, requestSize);
    }).detach();
    ++tag;
  }

  // tag = tag_base;
  // for (std::size_t i = 0; i < packagesLength; i++) {
  // Package package = Receive(ucp_worker, tag, requestSize);
  // std::cout << '[' << std::hex << std::this_thread::get_id()
  //<< "] Message received: " << package << std::endl;
  //++tag;
  //}

  std::this_thread::sleep_for(std::chrono::seconds(1));
}

void ReceiverCall(const UcpWorkerAddress &peerUcpWorkerAddress,
                  ucp_worker_h ucp_worker,
                  const std::size_t requestSize,
                  const std::size_t testStringLength) {
  ucp_tag_t tag = tag_base;
  for (std::size_t i = 0; i < packagesLength; i++) {
    Package package = Receive(ucp_worker, tag, requestSize);
    std::cout << '[' << std::hex << std::this_thread::get_id()
              << "] Message received: " << package << std::endl;
    ++tag;
  }

  // ucp_ep_h ucp_ep = CreateUcpEp(ucp_worker, peerUcpWorkerAddress);
  // std::vector<Package> packages =
  // MakePackagesForSending(packagesLength, testStringLength);
  // tag = tag_base;
  // for (Package &package : packages) {
  // std::thread([&package, tag, ucp_ep, ucp_worker, requestSize]() {
  // Send(package, ucp_ep, ucp_worker, tag, requestSize);
  //}).detach();
  //++tag;
  //}

  // std::this_thread::sleep_for(std::chrono::seconds(1));
}

//- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

int main(int argc, char *argv[]) {
  /**
   * Compile: g++ -std=c++17 -pthread demo2.cc -lucp -lucs
   * Usage
   *   for sender: demo
   *   for receiver: demo localhost
   */
  if (argc > 2) {
    std::cout << "Usage" << std::endl
              << "\tfor sender: " << argv[0] << std::endl
              << "\tfor receiver: " << argv[0] << " hostname" << std::endl;
    return -1;
  }

  if (argc < 2) {
    Run(SenderCall, AddressExchanger::MakeForSender(exchangingPort));
  } else {
    Run(ReceiverCall,
        AddressExchanger::MakeForReceiver(exchangingPort, argv[1]));
  }

  ////////////////////////////////////////////////////////////////////////

  // std::thread senderThread([]() {
  // Run(SenderCall, AddressExchanger::MakeForSender(exchangingPort));
  //});

  // std::thread receiverThread([]() {
  // Run(ReceiverCall,
  // AddressExchanger::MakeForReceiver(exchangingPort, "localhost"));
  //});

  // senderThread.join();
  // receiverThread.join();

  ////////////////////////////////////////////////////////////////////////

  // std::thread senderThread([]() {
  // Run(SenderCall, AddressExchanger::MakeForSender(exchangingPort));
  //});

  // std::thread receiverThread([]() {
  // Run(ReceiverCall,
  // AddressExchanger::MakeForReceiver(exchangingPort, "localhost"));
  //});

  // senderThread.detach();
  // receiverThread.detach();

  // std::this_thread::sleep_for(std::chrono::seconds(2));

  ////////////////////////////////////////////////////////////////////////

  // pid_t pid = fork();
  // if (pid) {
  // Run(ReceiverCall,
  // AddressExchanger::MakeForReceiver(exchangingPort, "localhost"));
  //} else {
  // Run(SenderCall, AddressExchanger::MakeForSender(exchangingPort));
  //}

  // std::this_thread::sleep_for(std::chrono::seconds(2));

  return 0;
}
