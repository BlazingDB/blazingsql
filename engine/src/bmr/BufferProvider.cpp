#include "BufferProvider.h"

#include <mutex>
#include <cuda.h>
#include <cuda_runtime.h>

namespace ral{
namespace memory{

  //TODO:JENS
  //make this be able to receive  a ucp worker which you will
  //need for the map an umap routines
pinned_allocator::pinned_allocator(bool use_ucx) : 
use_ucx{use_ucx} {

}


void base_allocator::allocate(void ** ptr, std::size_t size){
  do_allocate(ptr,size);
}

void base_allocator::deallocate(void * ptr){
  do_deallocate(ptr);
}

void host_allocator::do_allocate(void ** ptr, std::size_t size){
  
  *ptr = aligned_alloc( BLAZING_ALIGNMENT, size );
  if (!ptr) {
    throw std::runtime_error("Couldn't perform host allocation.");
  }
}

void pinned_allocator::do_allocate(void ** ptr, std::size_t size){
  cudaError_t err = cudaMallocHost(ptr, size);
  if (err != cudaSuccess) {
    throw std::runtime_error("Couldn't perform pinned allocation.");
  }
  //TODO:JENS
  //call mem_map here if using ucx 
}

void host_allocator::do_deallocate(void * ptr){
  free(ptr);
}

void pinned_allocator::do_deallocate(void * ptr){
  //TODO:JENS
  //call mem_umap here if using ucx
  auto err = cudaFreeHost(ptr);
  if (err != cudaSuccess) {
    throw std::runtime_error("Couldn't free pinned allocation.");
  }
}


allocation_pool::allocation_pool(std::unique_ptr<base_allocator> allocator, std::size_t size_buffers, std::size_t num_buffers) :
num_buffers (num_buffers), buffer_size(size_buffers), buffer_counter(num_buffers), allocator(std::move(allocator)) {
    
    this->allocations.push_back(std::make_unique<blazing_allocation>());
    try{
        this->allocator->allocate((void **) &allocations[0]->data,this->num_buffers * size_buffers);
    }catch(std::exception & e){
        throw;
    }
  allocations[0]->size = this->num_buffers * size_buffers;
  allocations[0]-> pool = this;
  for (int buffer_index = 0; buffer_index < this->num_buffers; buffer_index++) {
    
    auto buffer = std::make_unique<blazing_allocation_chunk>();
    buffer->size = this->buffer_size;
    buffer->data = allocations[0]->data + buffer_index * this->buffer_size;
    this->allocations[0]->allocation_chunks.push(std::move(buffer));
  }
  this->buffer_counter =  this->num_buffers;
  this->allocation_counter = 0;
}

allocation_pool::~allocation_pool(){
  free_all();
}

// TODO: consider adding some kind of priority
// based on when the request was made

std::unique_ptr<blazing_allocation_chunk> allocation_pool::get_chunk() {
  std::unique_lock<std::mutex> lock(in_use_mutex);
  
  bool found_mem = false;
  for(auto & allocation : allocations){
    if(!allocation->allocation_chunks.empty()){
        found_mem = true;
    }
  }
  if(! found_mem){
    this->grow(); //only  one thread can dispatch this at a time, the rest should wait on some
                //condition variable

  }
  for(auto & allocation : allocations){
    if(!allocation->allocation_chunks.empty()){
        this->allocation_counter++;
        auto temp = std::move(allocation->allocation_chunks.top());
        allocation->allocation_chunks.pop();
        return std::move(temp);
    }
  }
  //TODO: make exception for this
  throw std::runtime_error("Blazing allocation pool failed to grow or allocate.");
}



void allocation_pool::grow() {

  allocations.push_back(std::make_unique<blazing_allocation>());
  std::size_t num_new_buffers = this->num_buffers/2;
  auto last_index = allocations.size() -1;
  try{
    allocator->allocate((void **) &allocations[last_index]->data,num_new_buffers * buffer_size);
    for (int buffer_index = 0; buffer_index < num_new_buffers; buffer_index++) {
       auto buffer = std::make_unique<blazing_allocation_chunk>();
      buffer->size = this->buffer_size;
      buffer->data = allocations[last_index]->data + buffer_index * this->buffer_size;
      this->allocations[last_index]->allocation_chunks.push(std::move(buffer));
      this->buffer_counter++;
    }
    allocations[last_index]->size = num_new_buffers * buffer_size;
    allocations[last_index]-> pool = this;
  }catch(std::exception & e){
    throw;
  }





}


void allocation_pool::free_chunk(std::unique_ptr<blazing_allocation_chunk> buffer) {
  std::unique_lock<std::mutex> lock(in_use_mutex);
  buffer->allocation->allocation_chunks.push(std::move(buffer));
  this->allocation_counter--;
}


void allocation_pool::free_all() {
  std::unique_lock<std::mutex> lock(in_use_mutex);
  this->buffer_counter = 0;
  for(auto & allocation : allocations){
    while (false == allocation->allocation_chunks.empty()) {
      auto buffer = std::move(allocation->allocation_chunks.top());
      allocation->allocation_chunks.pop();
    }
    allocator->deallocate(allocation->data);
  }
  allocations.resize(0);
    this->allocation_counter = 0;
}

std::size_t allocation_pool::size_buffers() { return this->buffer_size; }


static std::shared_ptr<allocation_pool> host_buffer_instance{};
static std::shared_ptr<allocation_pool> pinned_buffer_instance{};


//TODO:JENS
//pass in ucp worker here to use on pinned buffer allocator
void set_allocation_pools(std::size_t size_buffers_host, std::size_t num_buffers_host,
std::size_t size_buffers_pinned, std::size_t num_buffers_pinned, bool map_ucx) {
  auto host_alloc = std::make_unique<host_allocator>(false);

  host_buffer_instance = std::make_shared<allocation_pool>(
   std::move(host_alloc) ,size_buffers_host,num_buffers_host);
  pinned_buffer_instance = std::make_shared<allocation_pool>(
    //TODO:JENS its this pinned allocator which will need to take in the worker 
    std::make_unique<pinned_allocator>(map_ucx),size_buffers_host,num_buffers_host);
}

void empty_pools(){
  host_buffer_instance->free_all();
  pinned_buffer_instance->free_all();
}
std::size_t allocation_pool::get_allocated_buffers(){
  return allocation_counter;
}


std::size_t allocation_pool::get_total_buffers(){
  return buffer_counter;
}


std::shared_ptr<allocation_pool > get_host_buffer_provider(){
  return host_buffer_instance;
}
std::shared_ptr<allocation_pool > get_pinned_buffer_provider(){
  return pinned_buffer_instance;
}

}
}