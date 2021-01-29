#pragma once

#include <vector>
#include <string>
#include <stack>
#include <mutex>
#include <memory>

namespace ral{

namespace memory{


/**
 * The wa
 * 
 */


using Buffer = std::basic_string<char>;


struct blazing_allocation_chunk;

struct blazing_allocation{

    std::size_t size;
    char *data;    
    std::stack< std::unique_ptr<blazing_allocation_chunk> > allocation_chunks;
    base_allocator * pool;
};

struct blazing_allocation_chunk{
    std::size_t size;
    char *data;    
    blazing_allocation * allocation;
};



struct blazing_chunked_buffer {
    std::vector<size_t> chunk_index; //the index of the chunk this maps to
    std::vector<size_t> offset; //the offset into each chunk to map to
    std::vector<size_t> size; //the size of each chunk
    size_t use_size;
};

#define BLAZING_ALIGNMENT 64u

class base_allocator{
public:
    base_allocator() {}
    void allocate(void ** ptr, std::size_t size);
    void deallocate(void * ptr);

protected:
    virtual void do_allocate(void ** ptr, std::size_t size) = 0;
    virtual void do_deallocate(void * ptr) = 0;
};

class host_allocator : public base_allocator {
public:
    host_allocator(bool use_ucx) {}
protected:
    void do_allocate(void ** ptr, std::size_t size);
    void do_deallocate(void * ptr);
};

class pinned_allocator : public base_allocator {
public:
    pinned_allocator(bool use_ucx);
protected:
    void do_allocate(void ** ptr, std::size_t size);
    void do_deallocate(void * ptr);
    bool use_ucx;
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

  int allocation_counter;
    
  std::vector<std::unique_ptr<blazing_allocation> > allocations;

  std::unique_ptr<base_allocator> allocator;
};


std::shared_ptr<allocation_pool > get_host_buffer_provider();
std::shared_ptr<allocation_pool > get_pinned_buffer_provider();

void set_allocation_pools(std::size_t size_buffers_host, std::size_t num_buffers_host,
std::size_t size_buffers_pinned, std::size_t num_buffers_pinned, bool map_ucx);
void empty_pools();
} //namespace memory



} //namespace ral
