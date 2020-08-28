#include "tests/utilities/BlazingUnitTest.h"
#include "communication/CommunicationInterface/node.hpp"
#include "communication/CommunicationInterface/messageReceiver.hpp"
#include "communication/CommunicationInterface/protocols.hpp"
#include "communication/CommunicationInterface/messageSender.hpp"
#include "execution_graph/logic_controllers/CacheMachine.h"
#include "execution_graph/logic_controllers/taskflow/kernel.h"
#include "execution_graph/logic_controllers/taskflow/graph.h"
#include "execution_graph/logic_controllers/BatchProcessing.h"

#include <memory>
#include <tests/utilities/base_fixture.hpp>
#include <cudf/table/table_view.hpp>
#include <cudf/sorting.hpp>
#include <cudf/copying.hpp>
#include <cudf/column/column_factories.hpp>
#include <tests/utilities/column_utilities.hpp>
#include <tests/utilities/type_lists.hpp>
#include <tests/utilities/column_wrapper.hpp>
#include <tests/utilities/table_utilities.hpp>

#include <ucp/api/ucp.h>
#include <ucs/type/status.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <netinet/in.h>
#include <netdb.h>
#include <pthread.h> /* pthread_self */
#include <thread>

using namespace ral::frame;

#define CHKERR_ACTION(_cond, _msg, _action) \
    do { \
        if (_cond) { \
            fprintf(stderr, "Failed to %s\n", _msg); \
            _action; \
        } \
    } while (0)


#define CHKERR_JUMP(_cond, _msg, _label) \
    CHKERR_ACTION(_cond, _msg, goto _label)


#define CHKERR_JUMP_RETVAL(_cond, _msg, _label, _retval) \
    do { \
        if (_cond) { \
            fprintf(stderr, "Failed to %s, return value %d\n", _msg, _retval); \
            goto _label; \
        } \
    } while (0)


enum ucp_test_mode_t {
    TEST_MODE_PROBE,
    TEST_MODE_WAIT,
    TEST_MODE_EVENTFD
};

/**
 * A struct that lets us access the request that the end points ucx-py generates.
 */
struct ucx_request {
	//!!!!!!!!!!! do not modify this struct this has to match what is found in
	// https://github.com/rapidsai/ucx-py/blob/branch-0.15/ucp/_libs/ucx_api.pyx
	// Make sure to check on the latest branch !!!!!!!!!!!!!!!!!!!!!!!!!!!
	int completed; /**< Completion flag that we do not use. */
	int uid;	   /**< We store a map of request uid ==> buffer_transport to manage completion of send */
};

static void request_init(void *request)
{
  struct ucx_request *req = (struct ucx_request *)request;

  req->completed = 0;
}

static void failure_handler(void *arg, ucp_ep_h ep, ucs_status_t status)
{
  ucs_status_t *arg_status = (ucs_status_t *)arg;

  // printf("[0x%x] failure handler called with status %d (%s)\n",
  //         (unsigned int)pthread_self(), status, ucs_status_string(status));
  printf("[0x%x] failure handler called with status %d\n",
          (unsigned int)pthread_self(), status);

  *arg_status = status;
}

int server_connect(uint16_t server_port)
{
  struct sockaddr_in inaddr;
  int lsock  = -1;
  int dsock  = -1;
  int optval = 1;
  int ret;

  lsock = socket(AF_INET, SOCK_STREAM, 0);
  CHKERR_JUMP(lsock < 0, "open server socket", err);

  optval = 1;
  ret = setsockopt(lsock, SOL_SOCKET, SO_REUSEADDR, &optval, sizeof(optval));
  CHKERR_JUMP(ret < 0, "server setsockopt()", err_sock);

  inaddr.sin_family      = AF_INET;
  inaddr.sin_port        = htons(server_port);
  inaddr.sin_addr.s_addr = INADDR_ANY;
  memset(inaddr.sin_zero, 0, sizeof(inaddr.sin_zero));
  ret = bind(lsock, (struct sockaddr*)&inaddr, sizeof(inaddr));
  CHKERR_JUMP(ret < 0, "bind server", err_sock);

  ret = listen(lsock, 0);
  CHKERR_JUMP(ret < 0, "listen server", err_sock);

  fprintf(stdout, "Waiting for connection...\n");

  /* Accept next connection */
  dsock = accept(lsock, NULL, NULL);
  CHKERR_JUMP(dsock < 0, "accept server", err_sock);

  close(lsock);

  return dsock;

err_sock:
  close(lsock);

err:
  return -1;
}

int client_connect(const char *server, uint16_t server_port)
{
  struct sockaddr_in conn_addr;
  struct hostent *he;
  int connfd;
  int ret;

  connfd = socket(AF_INET, SOCK_STREAM, 0);
  CHKERR_JUMP(connfd < 0, "open client socket", err);

  he = gethostbyname(server);
  CHKERR_JUMP((he == NULL || he->h_addr_list == NULL), "found a host", err_conn);

  conn_addr.sin_family = he->h_addrtype;
  conn_addr.sin_port   = htons(server_port);

  memcpy(&conn_addr.sin_addr, he->h_addr_list[0], he->h_length);
  memset(conn_addr.sin_zero, 0, sizeof(conn_addr.sin_zero));

  ret = connect(connfd, (struct sockaddr*)&conn_addr, sizeof(conn_addr));
  CHKERR_JUMP(ret < 0, "connect client", err_conn);

  return connfd;

err_conn:
  close(connfd);
err:
  return -1;
}

static char *client_target_name = "127.0.0.1";
static uint16_t server_port = 13337;
static ucs_status_t client_status = UCS_OK;

static thread_local ucp_address_t *local_addr;
static thread_local ucp_address_t *peer_addr;

static thread_local size_t local_addr_len;
static thread_local size_t peer_addr_len;

static thread_local int oob_sock = -1;

/* UCP handler objects */
static thread_local ucp_context_h ucp_context;
static thread_local ucp_worker_h ucp_worker;
static thread_local ucp_ep_h ucp_conn_ep;

int create_ucp_worker_and_ep(bool is_client) {
  ucp_test_mode_t ucp_test_mode = TEST_MODE_PROBE;

  /* UCP temporary vars */
  ucp_params_t ucp_params;
  ucp_worker_params_t worker_params;
  ucp_config_t *config;
  ucs_status_t status;
  /* OOB connection vars */
  uint64_t addr_len = 0;
  int ret = -1;
  memset(&ucp_params, 0, sizeof(ucp_params));
  memset(&worker_params, 0, sizeof(worker_params));

  /* UCP initialization */
  status = ucp_config_read(NULL, NULL, &config);
  CHKERR_JUMP(status != UCS_OK, "ucp_config_read\n", err);
  ucp_params.field_mask   = UCP_PARAM_FIELD_FEATURES |
                            UCP_PARAM_FIELD_REQUEST_SIZE |
                            UCP_PARAM_FIELD_REQUEST_INIT;
  ucp_params.features     = UCP_FEATURE_TAG;
  if (ucp_test_mode == TEST_MODE_WAIT || ucp_test_mode == TEST_MODE_EVENTFD) {
      ucp_params.features |= UCP_FEATURE_WAKEUP;
  }
  ucp_params.request_size    = sizeof(struct ucx_request);
  ucp_params.request_init    = request_init;
  status = ucp_init(&ucp_params, config, &ucp_context);
  // ucp_config_print(config, stdout, NULL, UCS_CONFIG_PRINT_CONFIG);
  ucp_config_release(config);
  CHKERR_JUMP(status != UCS_OK, "ucp_init\n", err);
  ucp_context_attr_t attr;
  status =  ucp_context_query(ucp_context, &attr);
  CHKERR_JUMP(status != UCS_OK, "ucp_context_query\n", err);
  
  worker_params.field_mask  = UCP_WORKER_PARAM_FIELD_THREAD_MODE;
  worker_params.thread_mode = UCS_THREAD_MODE_SINGLE;
  status = ucp_worker_create(ucp_context, &worker_params, &ucp_worker);
  CHKERR_JUMP(status != UCS_OK, "ucp_worker_create\n", err_cleanup);
  status = ucp_worker_get_address(ucp_worker, &local_addr, &local_addr_len);
  CHKERR_JUMP(status != UCS_OK, "ucp_worker_get_address\n", err_worker);

  // printf("[0x%x] local address length: %lu\n",
  //        (unsigned int)pthread_self(), local_addr_len);

  /* OOB connection establishment */
  if (is_client) {
    std::this_thread::sleep_for(4s);
    oob_sock = client_connect(client_target_name, server_port);
    CHKERR_JUMP(oob_sock < 0, "client_connect\n", err_addr);
    ret = recv(oob_sock, &addr_len, sizeof(addr_len), MSG_WAITALL);
    CHKERR_JUMP_RETVAL(ret != (int)sizeof(addr_len),
                        "receive server address length\n", err_addr, ret);
    peer_addr_len = addr_len;
    peer_addr = (ucp_address_t *)malloc(peer_addr_len);
    CHKERR_JUMP(!peer_addr, "allocate memory\n", err_addr);
    ret = recv(oob_sock, peer_addr, peer_addr_len, MSG_WAITALL);
    CHKERR_JUMP_RETVAL(ret != (int)peer_addr_len,
                        "receive server address\n", err_peer_addr, ret);

    addr_len = local_addr_len;
    ret = send(oob_sock, &addr_len, sizeof(addr_len), 0);
    CHKERR_JUMP_RETVAL(ret != (int)sizeof(addr_len),
                        "send client address length\n", err_peer_addr, ret);
    ret = send(oob_sock, local_addr, local_addr_len, 0);
    CHKERR_JUMP_RETVAL(ret != (int)local_addr_len, "send client address\n",
                        err_peer_addr, ret);

    ucp_ep_params_t ep_params;
    ep_params.field_mask      = UCP_EP_PARAM_FIELD_REMOTE_ADDRESS |
                                UCP_EP_PARAM_FIELD_ERR_HANDLING_MODE;
    ep_params.address         = peer_addr;
    ep_params.err_mode        = UCP_ERR_HANDLING_MODE_NONE;

    status = ucp_ep_create(ucp_worker, &ep_params, &ucp_conn_ep);
    CHKERR_JUMP(status != UCS_OK, "ucp_ep_create client\n", err_peer_addr);
  } else {
    oob_sock = server_connect(server_port);
    CHKERR_JUMP(oob_sock < 0, "server_connect\n", err_peer_addr);
    addr_len = local_addr_len;
    ret = send(oob_sock, &addr_len, sizeof(addr_len), 0);
    CHKERR_JUMP_RETVAL(ret != (int)sizeof(addr_len),
                        "send server address length\n", err_peer_addr, ret);
    ret = send(oob_sock, local_addr, local_addr_len, 0);
    CHKERR_JUMP_RETVAL(ret != (int)local_addr_len, "send server address\n",
                        err_peer_addr, ret);

    ret = recv(oob_sock, &addr_len, sizeof(addr_len), MSG_WAITALL);
    CHKERR_JUMP_RETVAL(ret != (int)sizeof(addr_len),
                        "receive client address length\n", err_addr, ret);
    peer_addr_len = addr_len;
    peer_addr = (ucp_address_t *)malloc(peer_addr_len);
    CHKERR_JUMP(!peer_addr, "allocate memory\n", err_addr);
    ret = recv(oob_sock, peer_addr, peer_addr_len, MSG_WAITALL);
    CHKERR_JUMP_RETVAL(ret != (int)peer_addr_len,
                        "receive client address\n", err_peer_addr, ret);

    ucp_ep_params_t ep_params;
    ep_params.field_mask      = UCP_EP_PARAM_FIELD_REMOTE_ADDRESS |
                                UCP_EP_PARAM_FIELD_ERR_HANDLING_MODE |
                                UCP_EP_PARAM_FIELD_ERR_HANDLER |
                                UCP_EP_PARAM_FIELD_USER_DATA;
    ep_params.address         = peer_addr;
    ep_params.err_mode        = UCP_ERR_HANDLING_MODE_NONE;
    ep_params.err_handler.cb  = failure_handler;
    ep_params.err_handler.arg = NULL;
    ep_params.user_data       = &client_status;

    status = ucp_ep_create(ucp_worker, &ep_params, &ucp_conn_ep);
    CHKERR_JUMP(status != UCS_OK, "ucp_ep_create client\n", err_peer_addr);
  }

  return 0;

err_peer_addr:
  free(peer_addr);

err_addr:
  ucp_worker_release_address(ucp_worker, local_addr);

err_worker:
  ucp_worker_destroy(ucp_worker);

err_cleanup:
  ucp_cleanup(ucp_context);

err:
  return ret;
}

void cleanup() {
  free(peer_addr);
  close(oob_sock);
  ucp_worker_release_address(ucp_worker, local_addr);
  ucp_ep_destroy(ucp_conn_ep);
  ucp_worker_destroy(ucp_worker);
  ucp_cleanup(ucp_context);
}

static size_t query_id = 0;

std::shared_ptr<blazingdb::manager::Context> make_context() {
	std::vector<blazingdb::transport::Node> nodes;
	blazingdb::transport::Node master_node;
	std::string logicalPlan;
	std::map<std::string, std::string> config_options;

	return std::make_shared<Context>(query_id, nodes, master_node, logicalPlan, config_options);
}

std::shared_ptr<ral::cache::graph> create_graph(){
  auto graph = std::make_shared<ral::cache::graph>();
  auto context = make_context();
  auto kernel_filter = std::make_shared<ral::batch::Filter>(0, "", context, graph);
  auto kernel_project = std::make_shared<ral::batch::Projection>(1, "", context, graph);

  ral::cache::cache_settings cache_machine_config;
  cache_machine_config.context = context->clone();
  graph->add_edge(kernel_filter, kernel_project, "0", "1", cache_machine_config);

  // auto inputCacheMachine = std::make_shared<ral::cache::CacheMachine>(context);
	// auto outputCacheMachine = std::make_shared<ral::cache::CacheMachine>(context);
	// kernel->input_.register_cache("1", inputCacheMachine);
	// kernel->output_.register_cache("1", outputCacheMachine);

  return graph;
}

struct MessageSendReceiveTest : public BlazingUnitTest {
  MessageSendReceiveTest() {}

  ~MessageSendReceiveTest() {}
};

TEST_F(MessageSendReceiveTest, send_receive_test) {
  auto thread = std::thread([]{
        std::this_thread::sleep_for(std::chrono::milliseconds(500));
        ASSERT_TRUE(create_ucp_worker_and_ep(true) == 0);

        std::map<std::string, comm::node> nodes_info_map;
        nodes_info_map.emplace("client", comm::node(0, "client", ucp_conn_ep, ucp_worker));
        nodes_info_map.emplace("server", comm::node(1, "server", ucp_conn_ep, ucp_worker));
        comm::ucp_nodes_info::getInstance().init(nodes_info_map);

        cudf::test::fixed_width_column_wrapper<int> col1{{4, 5, 3, 5, 8, 5, 6}};
        cudf::table_view orig_table{{col1}};
        std::vector<std::string> columnNames = {"column_1"};

        auto output_cache = std::make_shared<ral::cache::CacheMachine>(nullptr);

        ral::cache::MetadataDictionary metadata;
        metadata.add_value(ral::cache::KERNEL_ID_METADATA_LABEL, 0);
        metadata.add_value(ral::cache::QUERY_ID_METADATA_LABEL, query_id);
        metadata.add_value(ral::cache::ADD_TO_SPECIFIC_CACHE_METADATA_LABEL, "false");
        metadata.add_value(ral::cache::CACHE_ID_METADATA_LABEL, "");
        metadata.add_value(ral::cache::SENDER_WORKER_ID_METADATA_LABEL, "client");
        metadata.add_value(ral::cache::WORKER_IDS_METADATA_LABEL, "server");
        metadata.add_value(ral::cache::MESSAGE_ID, "");

        output_cache->addCacheData(
                std::make_unique<ral::cache::GPUCacheDataMetaData>(std::make_unique<ral::frame::BlazingTable>(orig_table, columnNames), metadata),"",true);

        comm::message_sender::initialize_instance(output_cache, nodes_info_map, 1, ucp_worker, 0);
        comm::message_sender::get_instance()->run_polling();

        std::this_thread::sleep_for(std::chrono::milliseconds(1500));

        cleanup();

        // exit(testing::Test::HasFailure());
  });

  auto thread_2 = std::thread([]{
     ASSERT_TRUE(create_ucp_worker_and_ep(false) == 0);

        std::map<std::string, comm::node> nodes_info_map;
        nodes_info_map.emplace("client", comm::node(0, "client", ucp_conn_ep, ucp_worker));
        // nodes_info_map.emplace("server", comm::node(1, "server", nullptr, ucp_worker));
        comm::ucp_nodes_info::getInstance().init(nodes_info_map);

        auto output_cache = std::make_shared<ral::cache::CacheMachine>(nullptr);

        auto graph = create_graph();
        comm::graphs_info::getInstance().register_graph(query_id, graph);

        auto kernel_filter = graph->get_node(0);
        auto out_cache = kernel_filter->output_cache();

        comm::ucx_message_listener::initialize_message_listener(ucp_worker, 1);
        comm::ucx_message_listener::get_instance()->poll_begin_message_tag();

        std::this_thread::sleep_for(std::chrono::milliseconds(1500));
        //check data

        cleanup();
  });
  thread.join();
  thread_2.join();



  // cudf::test::fixed_width_column_wrapper<int> col1{{4, 5, 3, 5, 8, 5, 6}};
  // cudf::table_view orig_table{{col1}};

  // std::vector<std::string> columnNames = {"column_1"};

  // std::vector<std::size_t> buffer_sizes;
  // std::vector<const char *> raw_buffers;
  // std::vector<ColumnTransport> column_transports;
  // std::vector<std::unique_ptr<rmm::device_buffer>> temp_scope_holder;
  // std::tie(buffer_sizes, raw_buffers, column_transports, temp_scope_holder) = comm::serialize_gpu_message_to_gpu_containers(ral::frame::BlazingTableView{orig_table, columnNames});

  // std::vector<std::unique_ptr<rmm::device_buffer>> gpu_buffers(raw_buffers.size());
  // for (size_t i = 0; i < raw_buffers.size(); i++) {
  //   gpu_buffers[i] = std::make_unique<rmm::device_buffer>(raw_buffers[i], buffer_sizes[i]);
  // }

  // ral::cache::MetadataDictionary metadata;
  // auto output_cache = std::make_shared<ral::cache::CacheMachine>(nullptr);

  // comm::message_receiver msg_rcv(column_transports, metadata, output_cache);

  // for (size_t i = 0; i < gpu_buffers.size(); i++) {
  //   msg_rcv.add_buffer(std::move(*(gpu_buffers[i])), i);
  // }

  // auto pulled_table = output_cache->pullFromCache();

  // cudf::test::expect_tables_equal(orig_table, pulled_table->view());
}
