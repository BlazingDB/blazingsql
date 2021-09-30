
#include "tests/utilities/BlazingUnitTest.h"
#include <src/utilities/DebuggingUtils.h>

#include <cudf_test/column_wrapper.hpp>
#include "src/bmr/BufferProvider.h"



struct AllocationPoolTest : public BlazingUnitTest {

	AllocationPoolTest(){
		
	}
};

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


struct ucx_request {
//!!!!!!!!!!! do not modify this struct this has to match what is found in
// https://github.com/rapidsai/ucx-py/blob/branch-0.18/ucp/_libs/ucx_api.pyx
// Make sure to check on the latest branch !!!!!!!!!!!!!!!!!!!!!!!!!!!
    int completed;
    unsigned int uid;
};

static void request_init(void *request)
{
    struct ucx_request *req = (struct ucx_request *)request;
    req->completed = 0;
    req->uid = 0;
}

ucp_context_h CreateUcpContext() {
   ucp_config_t *config;
   ucs_status_t status = ucp_config_read(NULL, NULL, &config);
   CheckError(status != UCS_OK, "ucp_config_read", [](){});

   ucp_params_t ucp_params;
   std::memset(&ucp_params, 0, sizeof(ucp_params));
   ucp_params.field_mask = UCP_PARAM_FIELD_FEATURES |
                           UCP_PARAM_FIELD_REQUEST_SIZE |
                           UCP_PARAM_FIELD_REQUEST_INIT;
   ucp_params.features = UCP_FEATURE_TAG | UCP_FEATURE_WAKEUP;
   ucp_params.request_size = sizeof(ucx_request);
   ucp_params.request_init = request_init;

   ucp_context_h ucp_context;
   status = ucp_init(&ucp_params, config, &ucp_context);

   const bool hasPrintUcpConfig = false;
   if (hasPrintUcpConfig) {
     ucp_config_print(config, stdout, NULL, UCS_CONFIG_PRINT_CONFIG);
   }

   ucp_config_release(config);
   CheckError(status != UCS_OK, "ucp_init", [](){});

   return ucp_context;
 }


TEST_F(AllocationPoolTest, initialize_test) {
	std::size_t size_buffers_host = 4000000;
	std::size_t num_buffers_host = 100;
	std::size_t size_buffers_pinned = 4000000;
	std::size_t num_buffers_pinned = 100;
	bool map_ucx = false;

    auto context = CreateUcpContext();
	ral::memory::set_allocation_pools(size_buffers_host, num_buffers_host,
        size_buffers_pinned, num_buffers_pinned, map_ucx, context);

	ral::memory::empty_pools();
    ucp_cleanup(context);
}	


TEST_F(AllocationPoolTest, mem_map_test) {
	std::size_t size_buffers_host = 4000000;
	std::size_t num_buffers_host = 100;
	std::size_t size_buffers_pinned = 4000000;
	std::size_t num_buffers_pinned = 100;
	bool map_ucx = true;

    auto context = CreateUcpContext();
	ral::memory::set_allocation_pools(size_buffers_host, num_buffers_host,
	size_buffers_pinned, num_buffers_pinned, map_ucx, context);

  std::unique_ptr<ral::memory::blazing_allocation_chunk> allocation_chunk = ral::memory::buffer_providers::get_pinned_buffer_provider()->get_chunk();
    ucp_mem_h handle = allocation_chunk->allocation->mem_handle;
    ucp_mem_attr_t attr;
    std::memset(&attr, 0, sizeof(ucp_mem_attr_t));
    // check that it is mapped
    attr.field_mask = UCP_MEM_ATTR_FIELD_ADDRESS |
                      UCP_MEM_ATTR_FIELD_LENGTH;
    ucs_status_t status = ucp_mem_query(handle, &attr);
    CheckError(status != UCS_OK, "ucp_mem_query", [context]() { ucp_cleanup(context);});
    ASSERT_TRUE(attr.field_mask & UCP_MEM_ATTR_FIELD_ADDRESS);
    ASSERT_TRUE(attr.field_mask & UCP_MEM_ATTR_FIELD_LENGTH);
    ASSERT_TRUE(attr.address != 0);
    ASSERT_TRUE(attr.length != 0);
    ral::memory::empty_pools();
}


TEST_F(AllocationPoolTest, get_chuck_free_chunk) {
	std::size_t size_buffers_host = 1000000;
	std::size_t num_buffers_host = 100;
	std::size_t size_buffers_pinned = 1000000;
	std::size_t num_buffers_pinned = 100;
	bool map_ucx = true;

  auto context = CreateUcpContext();
	ral::memory::set_allocation_pools(size_buffers_host, num_buffers_host,
	size_buffers_pinned, num_buffers_pinned, map_ucx, context);

  // lets make some buffers
  std::vector<std::unique_ptr<ral::memory::blazing_allocation_chunk> > raw_buffers0, raw_buffers1, raw_buffers2;
  for (int i = 0; i < num_buffers_pinned; i++){
    raw_buffers0.push_back(std::move(ral::memory::buffer_providers::get_pinned_buffer_provider()->get_chunk()));
  }
  for (int i = 0; i < num_buffers_pinned; i++){
    raw_buffers1.push_back(std::move(ral::memory::buffer_providers::get_pinned_buffer_provider()->get_chunk()));
  }
  for (int i = 0; i < num_buffers_pinned; i++){
    raw_buffers2.push_back(std::move(ral::memory::buffer_providers::get_pinned_buffer_provider()->get_chunk()));
  }

  // lets free them in a different order and make sure we handle that correctly
  for(auto i = 0; i < raw_buffers2.size(); i++){
    auto pool = raw_buffers2[i]->allocation->pool;
    pool->free_chunk(std::move(raw_buffers2[i]));
  }
  for(auto i = 0; i < raw_buffers1.size(); i++){
    auto pool = raw_buffers1[i]->allocation->pool;
    pool->free_chunk(std::move(raw_buffers1[i]));
  }
  for(auto i = 0; i < raw_buffers0.size(); i++){
    auto pool = raw_buffers0[i]->allocation->pool;
    pool->free_chunk(std::move(raw_buffers0[i]));
  }
  ASSERT_TRUE(true);


}


