#pragma once

#include <vector>
#include <string>
#include <stack>
#include <mutex>
#include <memory>

#include <ucp/api/ucp.h>

namespace ral{

namespace memory{


/**
 * The wa
 * 
 */


using Buffer = std::basic_string<char>;

// forward declarations
struct blazing_allocation_chunk;
class base_allocator;
class allocation_pool;


struct blazing_allocation{
    std::size_t index; // index inside the pool
    std::size_t size; // the size in bytes of the allocation
    std::size_t total_number_of_chunks; // number of chunks when originally created
    char *data;    // the pointer to the allocated memory
    std::stack< std::unique_ptr<blazing_allocation_chunk> > allocation_chunks; // These are the available chunks that are part of the allocation. 
    allocation_pool * pool;  // this is the pool that was used to make this allocation, and therefore this is what we would use to free it
    ucp_mem_h mem_handle;  // this is a memhandle used by UCX
};

struct blazing_allocation_chunk{
    std::size_t size;
    char *data;    
    blazing_allocation * allocation; // this is to know who made it
};



struct blazing_chunked_column_info {
    std::vector<size_t> chunk_index; //the index of the chunk this maps to
    std::vector<size_t> offset; //the offset into each chunk to map to
    std::vector<size_t> size; //the size of each chunk
    size_t use_size;
};

#define BLAZING_ALIGNMENT 64u

class base_allocator{
public:
    base_allocator() {}
    void allocate(void ** ptr, std::size_t size, ucp_mem_h * mem_handle_ptr);
    void deallocate(void * ptr, ucp_mem_h mem_handle);

protected:
    virtual void do_allocate(void ** ptr, std::size_t size, ucp_mem_h * mem_handle_ptr) = 0;
    virtual void do_deallocate(void * ptr, ucp_mem_h mem_handle) = 0;
};

class host_allocator : public base_allocator {
public:
    host_allocator(bool use_ucx) {}
protected:
    void do_allocate(void ** ptr, std::size_t size, ucp_mem_h * mem_handle_ptr);
    void do_deallocate(void * ptr, ucp_mem_h mem_handle);
};

class pinned_allocator : public base_allocator {
public:
    pinned_allocator();

    void setUcpContext(ucp_context_h context);

protected:
    void do_allocate(void ** ptr, std::size_t size, ucp_mem_h * mem_handle_ptr);
    void do_deallocate(void * ptr, ucp_mem_h mem_handle);
    bool use_ucx;
    ucp_context_h context;
};

class allocation_pool {
public:
  allocation_pool(std::unique_ptr<base_allocator> allocator, std::size_t size_buffers, std::size_t num_buffers);

  ~allocation_pool();

  std::unique_ptr<blazing_allocation_chunk> get_chunk();

  void free_chunk(std::unique_ptr<blazing_allocation_chunk> allocation);

  std::size_t size_buffers();

  void free_all();

  std::size_t get_allocated_buffers();
  std::size_t get_total_buffers();
private:
  // Its not threadsafe and the lock needs to be applied before calling it
  void grow();

  std::mutex in_use_mutex;

  bool use_ucx;

  std::size_t buffer_size;

  std::size_t num_buffers;

  int buffer_counter;

  std::vector<std::unique_ptr<blazing_allocation> > allocations;

  std::unique_ptr<base_allocator> allocator;
};

std::pair< std::vector<ral::memory::blazing_chunked_column_info>, std::vector<std::unique_ptr<ral::memory::blazing_allocation_chunk> >> convert_gpu_buffers_to_chunks(
    std::vector<std::size_t> buffer_sizes,bool use_pinned);

/**
	@brief This class represents the buffer providers used by the engine.
  @note Myers' singleton. Thread safe and unique. Note: C++11 required.
*/
class buffer_providers {
public:
  static std::shared_ptr<allocation_pool > & get_host_buffer_provider() {
    static std::shared_ptr<allocation_pool> host_buffer_instance{};
    return host_buffer_instance;
  }

  static std::shared_ptr<allocation_pool > & get_pinned_buffer_provider(){
    static std::shared_ptr<allocation_pool> pinned_buffer_instance{};
    return pinned_buffer_instance;
  }
};

// this function is what originally initialized the pinned memory and host memory allocation pools
void set_allocation_pools(std::size_t size_buffers_host, std::size_t num_buffers_host,
    std::size_t size_buffers_pinned, std::size_t num_buffers_pinned, bool map_ucx,
    ucp_context_h context);
void empty_pools();
} //namespace memory



} //namespace ral
