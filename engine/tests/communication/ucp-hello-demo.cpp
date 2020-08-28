// Sin NBR: g++ -std=gnu++17 -g -I/opt/ucx/include -L/opt/ucx/lib -I/usr/local/cuda-10.0/include -L/usr/local/cuda-10.0/lib64 hello-demo.cpp -lucp -lucs -lcudart -lcuda
// Con NBR: g++ -std=gnu++17 -g -I/opt/ucx/include -L/opt/ucx/lib -I/usr/local/cuda-10.0/include -L/usr/local/cuda-10.0/lib64 hello-demo.cpp -lucp -lucs -lcudart -lcuda -DWITH_NBR
// Con CUDA: g++ -std=gnu++17 -g -I/opt/ucx/include -L/opt/ucx/lib -I/usr/local/cuda-10.0/include -L/usr/local/cuda-10.0/lib64 hello-demo.cpp -lucp -lucs -lcudart -lcuda -DWITH_NBR -DUSE_CUDA

// Para ejecutar server: ./a.out
// Para ejecutar client: ./a.out localhost

#include <iostream>
#include <memory>

#include <cstring>

#include <netdb.h>
#include <signal.h>
#include <unistd.h>

#include <ucp/api/ucp.h>
#include <ucs/memory/memory_type.h>

#ifdef USE_CUDA
#include <cuda.h>
#include <cuda_runtime.h>

#define CUDA_FUNC(_func)                                                       \
  do {                                                                         \
    cudaError_t _result = (_func);                                             \
    if (cudaSuccess != _result) {                                              \
      fprintf(stderr, "%s failed: %s\n", #_func, cudaGetErrorString(_result)); \
    }                                                                          \
  } while (0)
#endif

static inline int server_connect(std::uint16_t port) {
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
  memset(inaddr.sin_zero, 0, sizeof(inaddr.sin_zero));
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

  std::cout << "[0x" << std::hex << static_cast<unsigned int>(pthread_self())
            << "] Waiting for connection..." << std::endl;

  dsock = accept(lsock, NULL, NULL);
  if (dsock < 0) {
    std::cout << "accept server" << std::endl;
    close(lsock);
    throw std::runtime_error("accept server");
  }

  close(lsock);

  return dsock;
}

static inline int client_connect(const char *server, std::uint16_t port) {
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

  he = gethostbyname(server);
  if (he == NULL || he->h_addr_list == NULL) {
    const std::string message = "found a host";
    std::cout << message << std::endl;
    close(connfd);
    throw std::runtime_error(message);
  }

  conn_addr.sin_family = he->h_addrtype;
  conn_addr.sin_port = htons(port);

  memcpy(&conn_addr.sin_addr, he->h_addr_list[0], he->h_length);
  memset(conn_addr.sin_zero, 0, sizeof(conn_addr.sin_zero));

  ret = connect(connfd, (struct sockaddr *) &conn_addr, sizeof(conn_addr));
  if (ret < 0) {
    const std::string message = "connect client";
    std::cout << message << std::endl;
    close(connfd);
    throw std::runtime_error(message);
  }

  return connfd;
}

static inline int barrier(int sock) {
  int dummy = 0;
  ssize_t res;

  res = send(sock, &dummy, sizeof(dummy), 0);
  if (res < 0) { return res; }

  res = recv(sock, &dummy, sizeof(dummy), MSG_WAITALL);

  return !(res == sizeof(dummy));
}

static void usage(const std::string &name) {
  std::cout << "Usage: " << name << " [HOSTNAME]" << std::endl;
}

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


class AppContext {
public:
  bool completed;
};


static void request_init(void *request) {
  // std::cout << "Request init" << std::endl;
  AppContext *c = reinterpret_cast<AppContext *>(request);
  c->completed = 0;
}

static class ErrorHandling {
public:
  ucp_err_handling_mode_t ucp_err_mode;
  int failure;
} err_handling_opt;

class Msg {
public:
  std::uint64_t data_len;
};

static const ucp_tag_t tag = 0x1337a880u;
static const ucp_tag_t tag_mask = UINT64_MAX;

static void send_handler(void *request, ucs_status_t status) {
  AppContext *context = (AppContext *) request;

  context->completed = 1;

  printf("[0x%x] send handler called with status %d (%s)\n",
         (unsigned int) pthread_self(),
         status,
         ucs_status_string(status));
}

static void wait(ucp_worker_h ucp_worker, AppContext *context) {
  while (context->completed == 0) { ucp_worker_progress(ucp_worker); }
}

static ucs_memory_type_t test_mem_type =
#ifdef USE_CUDA
    UCS_MEMORY_TYPE_CUDA;
#else
    UCS_MEMORY_TYPE_HOST;
#endif

static inline void *mem_type_malloc(std::size_t length) {
  void *ptr;

  switch (test_mem_type) {
  case UCS_MEMORY_TYPE_HOST: ptr = malloc(length); break;
#ifdef USE_CUDA
  case UCS_MEMORY_TYPE_CUDA: CUDA_FUNC(cudaMalloc(&ptr, length)); break;
  case UCS_MEMORY_TYPE_CUDA_MANAGED:
    CUDA_FUNC(cudaMallocManaged(&ptr, length, cudaMemAttachGlobal));
    break;
#endif
  default:
    fprintf(stderr, "Unsupported memory type: %d\n", test_mem_type);
    ptr = NULL;
    break;
  }

  return ptr;
}

static inline void *mem_type_memset(void *dst, int value, std::size_t count) {
  switch (test_mem_type) {
  case UCS_MEMORY_TYPE_HOST: std::memset(dst, value, count); break;
#ifdef USE_CUDA
  case UCS_MEMORY_TYPE_CUDA:
  case UCS_MEMORY_TYPE_CUDA_MANAGED:
    CUDA_FUNC(cudaMemset(dst, value, count));
    break;
#endif
  default: fprintf(stderr, "Unsupported memory type: %d", test_mem_type); break;
  }

  return dst;
}

static inline void *
mem_type_memcpy(void *dst, const void *src, std::size_t count) {
  switch (test_mem_type) {
  case UCS_MEMORY_TYPE_HOST: memcpy(dst, src, count); break;
#ifdef USE_CUDA
  case UCS_MEMORY_TYPE_CUDA:
  case UCS_MEMORY_TYPE_CUDA_MANAGED:
    CUDA_FUNC(cudaMemcpy(dst, src, count, cudaMemcpyDefault));
    break;
#endif
  default:
    fprintf(stderr, "Unsupported memory type: %d\n", test_mem_type);
    break;
  }

  return dst;
}

static inline void mem_type_free(void *address) {
  switch (test_mem_type) {
  case UCS_MEMORY_TYPE_HOST: free(address); break;
#ifdef USE_CUDA
  case UCS_MEMORY_TYPE_CUDA:
  case UCS_MEMORY_TYPE_CUDA_MANAGED: CUDA_FUNC(cudaFree(address)); break;
#endif
  default:
    fprintf(stderr, "Unsupported memory type: %d\n", test_mem_type);
    break;
  }
}

static inline void
recv_handler(void *request, ucs_status_t status, ucp_tag_recv_info_t *info) {
  AppContext *context = (AppContext *) request;

  context->completed = 1;

  printf("[0x%x] receive handler called with status %d (%s), length %lu\n",
         (unsigned int) pthread_self(),
         status,
         ucs_status_string(status),
         info->length);
}

static int run_ucx_client(ucp_worker_h ucp_worker,
                          const ucp_address_t *peer_addr,
                          const std::size_t peer_addr_len,
                          const ucp_address_t *local_addr,
                          const std::size_t local_addr_len,
                          const std::size_t request_size) {
  ucp_ep_params_t ep_params;
  ep_params.field_mask =
      UCP_EP_PARAM_FIELD_REMOTE_ADDRESS | UCP_EP_PARAM_FIELD_ERR_HANDLING_MODE;
  ep_params.address = peer_addr;
  ep_params.err_mode = err_handling_opt.ucp_err_mode;

  ucs_status_t status;
  ucp_ep_h server_ep;
  status = ucp_ep_create(ucp_worker, &ep_params, &server_ep);
  CheckError(status != UCS_OK, "ucp_ep_create\n");

  Msg *msg;
  const std::size_t msg_len = sizeof(*msg) + local_addr_len;
  msg = reinterpret_cast<Msg *>(malloc(msg_len));
  CheckError(msg == NULL, "allocate memory\n", [&server_ep]() {
    ucp_ep_destroy(server_ep);
  });
  std::memset(msg, 0, msg_len);

  msg->data_len = local_addr_len;
  std::memcpy(msg + 1, local_addr, local_addr_len);

  AppContext *request = reinterpret_cast<AppContext *>(ucp_tag_send_nb(
      server_ep, msg, msg_len, ucp_dt_make_contig(1), tag, send_handler));

  if (UCS_PTR_IS_ERR(request)) {
    fprintf(stderr, "unable to send UCX address message\n");
    free(msg);
    throw std::runtime_error("unable to send UCX address message");
  } else if (UCS_PTR_IS_PTR(request)) {
    wait(ucp_worker, request);
    request->completed = 0;
    ucp_request_release(request);
  }

  free(msg);
  if (err_handling_opt.failure) {
    fprintf(stderr, "Emulating unexpected failure on client side\n");
    raise(SIGKILL);
  }

  static const char test_string[] = "Test String 14aaa";
  static const long test_string_length = sizeof(test_string);

#ifdef WITH_NBR
  const std::size_t msg_len2 = sizeof(*msg) + test_string_length;
  msg = reinterpret_cast<Msg *>(mem_type_malloc(msg_len2));
  CheckError(msg == NULL, "allocate memory\n");
  mem_type_memset(msg, 0, msg_len2);


  char *nbrrequest = reinterpret_cast<char *>(malloc(request_size));
  status = ucp_tag_recv_nbr(ucp_worker,
                            msg,
                            msg_len2,
                            ucp_dt_make_contig(1),
                            tag,
                            tag_mask,
                            nbrrequest + request_size);

  if (UCS_STATUS_IS_ERR(status)) {
    std::cerr << "ucp_tag_recv_nb returned status "
              << ucs_status_string(status);
    throw std::runtime_error("ucp_tag_recv_nb returned status");
  }


  do {
    ucp_worker_progress(ucp_worker);
    ucp_tag_recv_info_t info_tag;
    status = ucp_tag_recv_request_test(nbrrequest + request_size, &info_tag);
  } while (status == UCS_INPROGRESS);

  std::cout << "[0x" << std::hex << static_cast<unsigned int>(pthread_self())
            << "] Msg: " << reinterpret_cast<std::size_t>(msg + 1) << std::endl;

  char *str = reinterpret_cast<char *>(calloc(1, test_string_length));
  if (str != NULL) {
    mem_type_memcpy(str, msg + 1, test_string_length);
    std::cout << "[0x" << std::hex << static_cast<unsigned int>(pthread_self())
              << "] Message received: " << str << std::endl;
    free(str);
  } else {
    fprintf(stderr, "Memory allocation failed\n");
    ucp_ep_destroy(server_ep);
    throw std::runtime_error("Memory allocation failed");
  }

#else
  ucp_tag_message_h msg_tag;
  ucp_tag_recv_info_t info_tag;
  for (;;) {
    msg_tag = ucp_tag_probe_nb(ucp_worker, tag, tag_mask, 1, &info_tag);
    if (msg_tag != NULL) {
      break;
    } else if (ucp_worker_progress(ucp_worker)) {
      continue;
    }

    status = ucp_worker_wait(ucp_worker);
    CheckError(status != UCS_OK, "ucp_worker_wait\n", [&server_ep]() {
      ucp_ep_destroy(server_ep);
    });
  }

  msg = reinterpret_cast<Msg *>(mem_type_malloc(info_tag.length));
  CheckError(msg == NULL, "allocate memory\n", [&server_ep]() {
    ucp_ep_destroy(server_ep);
  });

  request =
      reinterpret_cast<AppContext *>(ucp_tag_msg_recv_nb(ucp_worker,
                                                         msg,
                                                         info_tag.length,
                                                         ucp_dt_make_contig(1),
                                                         msg_tag,
                                                         recv_handler));

  if (UCS_PTR_IS_ERR(request)) {
    fprintf(stderr,
            "unable to receive UCX data message (%u)\n",
            UCS_PTR_STATUS(request));
    free(msg);
    ucp_ep_destroy(server_ep);
    throw std::runtime_error("Unable to receive UCX data message");
  } else {
    CheckError(!UCS_PTR_IS_PTR(request), "request cannot be null");
    wait(ucp_worker, request);
    request->completed = 0;
    ucp_request_release(request);
    std::cout << "[0x" << std::hex << static_cast<unsigned int>(pthread_self())
              << "] UCX data message was received" << std::endl;
  }

  char *str = reinterpret_cast<char *>(calloc(1, test_string_length));
  if (str != NULL) {
    mem_type_memcpy(str, msg + 1, test_string_length);
    std::cout << "[0x" << std::hex << static_cast<unsigned int>(pthread_self())
              << "] Message received: " << str << std::endl;
    free(str);
  } else {
    fprintf(stderr, "Memory allocation failed\n");
    ucp_ep_destroy(server_ep);
    throw std::runtime_error("Memory allocation failed");
  }

  mem_type_free(msg);
#endif

  return 0;
}

static void failure_handler(void *arg, ucp_ep_h ep, ucs_status_t status) {
  ucs_status_t *arg_status = (ucs_status_t *) arg;

  printf("[0x%x] failure handler called with status %d (%s)\n",
         (unsigned int) pthread_self(),
         status,
         ucs_status_string(status));

  *arg_status = status;
}

static inline void set_msg_data_len(Msg *msg, std::uint64_t data_len) {
  mem_type_memcpy(&msg->data_len, &data_len, sizeof(data_len));
}

static inline void generate_test_string(char *str, int size) {
  char *tmp_str;
  int i;

  tmp_str = reinterpret_cast<char *>(calloc(1, size));
  CheckError(tmp_str == NULL, "allocate memory\n");

  for (i = 0; i < (size - 1); ++i) { tmp_str[i] = 'A' + (i % 26); }

  mem_type_memcpy(str, tmp_str, size);

  free(tmp_str);
}

static inline void flush_callback(void *request, ucs_status_t status) {
  std::cout << "Flush Callback" << std::endl;
}

static inline ucs_status_t flush_ep(ucp_worker_h worker, ucp_ep_h ep) {
  void *request;

  request = ucp_ep_flush_nb(ep, 0, flush_callback);
  if (request == NULL) {
    return UCS_OK;
  } else if (UCS_PTR_IS_ERR(request)) {
    return UCS_PTR_STATUS(request);
  } else {
    ucs_status_t status;
    do {
      ucp_worker_progress(worker);
      status = ucp_request_check_status(request);
    } while (status == UCS_INPROGRESS);
    ucp_request_release(request);
    return status;
  }
}

static int run_ucx_server(ucp_worker_h ucp_worker,
                          const std::size_t request_size) {
  ucp_tag_recv_info_t info_tag;

  ucp_tag_message_h msg_tag;
  do {
    ucp_worker_progress(ucp_worker);
    msg_tag = ucp_tag_probe_nb(ucp_worker, tag, tag_mask, 1, &info_tag);
  } while (msg_tag == NULL);

  Msg *msg;
  msg = reinterpret_cast<Msg *>(malloc(info_tag.length));
  CheckError(msg == NULL, "allocate memory\n");

  AppContext *request =
      reinterpret_cast<AppContext *>(ucp_tag_msg_recv_nb(ucp_worker,
                                                         msg,
                                                         info_tag.length,
                                                         ucp_dt_make_contig(1),
                                                         msg_tag,
                                                         recv_handler));

  if (UCS_PTR_IS_ERR(request)) {
    fprintf(stderr,
            "unable to receive UCX address message (%s)\n",
            ucs_status_string(UCS_PTR_STATUS(request)));
    free(msg);
    throw std::runtime_error("unable to receive UCX address message");
  } else {
    CheckError(!UCS_PTR_IS_PTR(request), "request cannot return null");
    wait(ucp_worker, request);
    request->completed = 0;
    ucp_request_release(request);
    std::cout << "[0x" << std::hex << static_cast<unsigned int>(pthread_self())
              << "] UCX address message was received" << std::endl
              << "[0x" << std::hex << static_cast<unsigned int>(pthread_self())
              << "] Msg: " << std::hex
              << *reinterpret_cast<std::size_t *>(msg + 1) << " "
              << msg->data_len << std::endl;
  }

  static ucp_address_t *peer_addr;
  static std::size_t peer_addr_len;

  peer_addr_len = msg->data_len;
  peer_addr = reinterpret_cast<ucp_address_t *>(malloc(peer_addr_len));
  if (peer_addr == NULL) {
    fprintf(stderr, "unable to allocate memory for peer address\n");
    free(msg);
    throw std::runtime_error("unable to allocate memory for peer address");
  }

  std::memcpy(peer_addr, msg + 1, peer_addr_len);
  free(msg);

  static const char test_string[] = "Test String 14aaa";
  static const long test_string_length = sizeof(test_string);

  std::size_t msg_len = sizeof(*msg) + test_string_length;
  msg = reinterpret_cast<Msg *>(mem_type_malloc(msg_len));
  CheckError(msg == NULL, "allocate memory\n");
  mem_type_memset(msg, 0, msg_len);

  set_msg_data_len(msg, msg_len - sizeof(*msg));

  try {
    generate_test_string((char *) (msg + 1), test_string_length);
  } catch (std::exception) {
    std::cerr << "generate test string" << std::endl;
    mem_type_free(msg);
    throw std::runtime_error("generate test string");
  }

  static ucs_status_t client_status = UCS_OK;
  ucp_ep_params_t ep_params;
  ep_params.field_mask =
      UCP_EP_PARAM_FIELD_REMOTE_ADDRESS | UCP_EP_PARAM_FIELD_ERR_HANDLING_MODE |
      UCP_EP_PARAM_FIELD_ERR_HANDLER | UCP_EP_PARAM_FIELD_USER_DATA;
  ep_params.address = peer_addr;
  ep_params.err_mode = err_handling_opt.ucp_err_mode;
  ep_params.err_handler.cb = failure_handler;
  ep_params.err_handler.arg = NULL;
  ep_params.user_data = &client_status;

  ucs_status_t status;
  ucp_ep_h client_ep;
  status = ucp_ep_create(ucp_worker, &ep_params, &client_ep);
  CheckError(status != UCS_OK, "ucp_ep_create\n");

#ifdef WITH_NBR
  char *nbrrequest = reinterpret_cast<char *>(malloc(request_size));
  status = ucp_tag_send_nbr(client_ep,
                            msg,
                            msg_len,
                            ucp_dt_make_contig(1),
                            tag,
                            nbrrequest + request_size);
  if (status != UCS_INPROGRESS) {
    std::cout << "\033[31m>>>>>>>>>>>>>>>>>>>>>>>>>INPROGRESS\033[0m"
              << std::endl;
  }

  do {
    ucp_worker_progress(ucp_worker);
    status = ucp_request_check_status(nbrrequest + request_size);
  } while (status == UCS_INPROGRESS);
#else
  request = reinterpret_cast<AppContext *>(ucp_tag_send_nb(
      client_ep, msg, msg_len, ucp_dt_make_contig(1), tag, send_handler));

  if (UCS_PTR_IS_ERR(request)) {
    fprintf(stderr, "unable to send UCX data message\n");
    mem_type_free(msg);
    ucp_ep_destroy(client_ep);
    throw std::runtime_error("unable to send UCX data message");
  } else if (UCS_PTR_IS_PTR(request)) {
    std::cout << "UCX data message was scheduled for send" << std::endl;
    wait(ucp_worker, request);
    request->completed = 0;
    ucp_request_release(request);
  }

  status = flush_ep(ucp_worker, client_ep);
  std::cout << "[0x" << std::hex << static_cast<unsigned int>(pthread_self())
            << "] ";
  printf("flush_ep completed with status %d (%s)\n",
         status,
         ucs_status_string(status));
#endif

  return 0;
}

int main(int argc, char *argv[]) {
  if (argc > 2) {
    usage(argv[0]);
    return -1;
  }

  ucs_status_t status;

  ucp_config_t *config;
  status = ucp_config_read(NULL, NULL, &config);
  CheckError(status != UCS_OK, "ucp_config_read\n");

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
  CheckError(status != UCS_OK, "ucp_init\n");

  ucp_worker_params_t worker_params;
  std::memset(&worker_params, 0, sizeof(worker_params));
  worker_params.field_mask = UCP_WORKER_PARAM_FIELD_THREAD_MODE;
  worker_params.thread_mode = UCS_THREAD_MODE_SINGLE;

  ucp_worker_h ucp_worker;
  status = ucp_worker_create(ucp_context, &worker_params, &ucp_worker);
  CheckError(status != UCS_OK, "ucp_worker_create\n", [&ucp_context]() {
    ucp_cleanup(ucp_context);
  });

  static ucp_address_t *local_addr;
  static std::size_t local_addr_len;

  status = ucp_worker_get_address(ucp_worker, &local_addr, &local_addr_len);
  CheckError(status != UCS_OK,
             "ucp_worker_get_address\n",
             [&ucp_worker, &ucp_context]() {
               ucp_worker_destroy(ucp_worker);
               ucp_cleanup(ucp_context);
             });

  std::cout << "[0x" << std::hex << (unsigned int) pthread_self()
            << "] local address length: "
            << *reinterpret_cast<std::size_t *>(local_addr) << ' '
            << local_addr_len << std::endl;

  int oob_sock = -1;
  std::uint64_t addr_len = 0;
  int ret = -1;
  std::uint16_t server_port = 9876;

  static ucp_address_t *peer_addr;
  static std::size_t peer_addr_len;

  if (argc > 1) {
    peer_addr_len = local_addr_len;

    oob_sock = client_connect(argv[1], server_port);
    CheckError(oob_sock < 0, "client_connect\n", [&ucp_worker, &ucp_context]() {
      ucp_worker_release_address(ucp_worker, local_addr);
      ucp_worker_destroy(ucp_worker);
      ucp_cleanup(ucp_context);
    });

    ret = recv(oob_sock, &addr_len, sizeof(addr_len), MSG_WAITALL);
    CheckError(ret != (int) sizeof(addr_len),
               "receive address length\n",
               [&ucp_worker, &ucp_context]() {
                 ucp_worker_release_address(ucp_worker, local_addr);
                 ucp_worker_destroy(ucp_worker);
                 ucp_cleanup(ucp_context);
               });

    peer_addr_len = addr_len;
    peer_addr = reinterpret_cast<ucp_address_t *>(malloc(peer_addr_len));
    CheckError(!peer_addr, "allocate memory\n", [&ucp_worker, &ucp_context]() {
      free(peer_addr);
      ucp_worker_release_address(ucp_worker, local_addr);
      ucp_worker_destroy(ucp_worker);
      ucp_cleanup(ucp_context);
    });

    ret = recv(oob_sock, peer_addr, peer_addr_len, MSG_WAITALL);
    CheckError(ret != (int) peer_addr_len,
               "receive address\n",
               [&ucp_worker, &ucp_context]() {
                 free(peer_addr);
                 ucp_worker_release_address(ucp_worker, local_addr);
                 ucp_worker_destroy(ucp_worker);
                 ucp_cleanup(ucp_context);
               });

    std::cout << "[0x" << std::hex << (unsigned int) pthread_self()
              << "] peer address length: "
              << *reinterpret_cast<std::size_t *>(peer_addr) << ' '
              << peer_addr_len << std::endl;
  } else {
    oob_sock = server_connect(server_port);
    CheckError(oob_sock < 0, "server_connect\n", [&ucp_worker, &ucp_context]() {
      ucp_worker_release_address(ucp_worker, local_addr);
      ucp_worker_destroy(ucp_worker);
      ucp_cleanup(ucp_context);
    });

    addr_len = local_addr_len;
    ret = send(oob_sock, &addr_len, sizeof(addr_len), 0);
    CheckError(ret != (int) sizeof(addr_len),
               "send address length\n",
               [&ucp_worker, &ucp_context]() {
                 ucp_worker_release_address(ucp_worker, local_addr);
                 ucp_worker_destroy(ucp_worker);
                 ucp_cleanup(ucp_context);
               });

    ret = send(oob_sock, local_addr, local_addr_len, 0);
    CheckError(ret != (int) local_addr_len,
               "send address\n",
               [&ucp_worker, &ucp_context]() {
                 ucp_worker_release_address(ucp_worker, local_addr);
                 ucp_worker_destroy(ucp_worker);
                 ucp_cleanup(ucp_context);
               });
  }

  ucp_context_attr_t attr;
  attr.field_mask = UCP_ATTR_FIELD_REQUEST_SIZE;

  status = ucp_context_query(ucp_context, &attr);
  CheckError(status != UCS_OK, "ucp_context_query\n");

  ret = argc > 1 ? run_ucx_client(ucp_worker,
                                  peer_addr,
                                  peer_addr_len,
                                  local_addr,
                                  local_addr_len,
                                  attr.request_size)
                 : run_ucx_server(ucp_worker, attr.request_size);
  close(oob_sock);
  if (!ret && !err_handling_opt.failure) { ret = barrier(oob_sock); }
  free(peer_addr);
  ucp_worker_release_address(ucp_worker, local_addr);
  ucp_worker_destroy(ucp_worker);
  ucp_cleanup(ucp_context);

  return ret;
}
