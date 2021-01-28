
#include "tests/utilities/BlazingUnitTest.h"
#include <src/utilities/DebuggingUtils.h>

#include <cudf_test/column_wrapper.hpp>
#include "src/bmr/BufferProvider.h"



struct AllocationPoolTest : public BlazingUnitTest {

	AllocationPoolTest(){
		
	}
};


//TODO:JENS
//you need to initialize the context with something like what is below. this is code i pulled from the message_send_receive.cpp test in the directory tests/message_send_receive.cpp

// struct ucx_request {
// 	//!!!!!!!!!!! do not modify this struct this has to match what is found in
// 	// https://github.com/rapidsai/ucx-py/blob/branch-0.18/ucp/_libs/ucx_api.pyx
// 	// Make sure to check on the latest branch !!!!!!!!!!!!!!!!!!!!!!!!!!!
// 	int completed; 
// 	int uid;	   
// };

// static void request_init(void *request)
// {
//   struct ucx_request *req = (struct ucx_request *)request;
//   req->completed = 0;
//   req->uid = -1;
// }

// //- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

// ucp_context_h CreateUcpContext() {
//   ucp_config_t *config;
//   ucs_status_t status = ucp_config_read(NULL, NULL, &config);
//   CheckError(status != UCS_OK, "ucp_config_read");

//   ucp_params_t ucp_params;
//   std::memset(&ucp_params, 0, sizeof(ucp_params));
//   ucp_params.field_mask = UCP_PARAM_FIELD_FEATURES |
//                           UCP_PARAM_FIELD_REQUEST_SIZE |
//                           UCP_PARAM_FIELD_REQUEST_INIT;
//   ucp_params.features = UCP_FEATURE_TAG | UCP_FEATURE_WAKEUP;
//   ucp_params.request_size = sizeof(ucx_request);
//   ucp_params.request_init = request_init;

//   ucp_context_h ucp_context;
//   status = ucp_init(&ucp_params, config, &ucp_context);

//   const bool hasPrintUcpConfig = false;
//   if (hasPrintUcpConfig) {
//     ucp_config_print(config, stdout, NULL, UCS_CONFIG_PRINT_CONFIG);
//   }

//   ucp_config_release(config);
//   CheckError(status != UCS_OK, "ucp_init");

//   return ucp_context;
// }

// ucp_worker_h CreatetUcpWorker(ucp_context_h ucp_context) {
//   ucp_worker_params_t worker_params;
//   std::memset(&worker_params, 0, sizeof(worker_params));
//   worker_params.field_mask = UCP_WORKER_PARAM_FIELD_THREAD_MODE;
//   worker_params.thread_mode = UCS_THREAD_MODE_MULTI;  // UCS_THREAD_MODE_SINGLE;

//   ucp_worker_h ucp_worker;
//   ucs_status_t status =
//       ucp_worker_create(ucp_context, &worker_params, &ucp_worker);
//   CheckError(status != UCS_OK, "ucp_worker_create", [&ucp_context]() {
//     ucp_cleanup(ucp_context);
//   });

//   return ucp_worker;
// }



TEST_F(AllocationPoolTest, initialize_test) {
	std::size_t size_buffers_host = 4000000;
	std::size_t num_buffers_host = 100;
	std::size_t size_buffers_pinned = 4000000;
	std::size_t num_buffers_pinned = 100;
	bool map_ucx = false;
	ral::memory::set_allocation_pools(size_buffers_host, num_buffers_host,
	size_buffers_pinned, num_buffers_pinned, map_ucx);
	ral::memory::empty_pools();
}	


//TODO:JENS
TEST_F(AllocationPoolTest, mem_map_test) {
	std::size_t size_buffers_host = 4000000;
	std::size_t num_buffers_host = 100;
	std::size_t size_buffers_pinned = 4000000;
	std::size_t num_buffers_pinned = 100;
	bool map_ucx = false;
	ral::memory::set_allocation_pools(size_buffers_host, num_buffers_host,
	size_buffers_pinned, num_buffers_pinned, map_ucx);

	//TODO:JENS
	//at this point the memory should be registered
	//you should be able to use  ucp_mem_query() to test this i think

	ral::memory::empty_pools();
}