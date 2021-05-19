#include "BufferProvider.h"

#include <iostream>
#include <mutex>
#include <cstring>
#include <cuda.h>
#include <cuda_runtime.h>

#include <ucs/type/status.h>

#include <spdlog/spdlog.h>
#include <spdlog/async.h>
#include <spdlog/sinks/basic_file_sink.h>
#include <spdlog/sinks/stdout_color_sinks.h>

namespace ral{
namespace memory{

pinned_allocator::pinned_allocator() :
use_ucx{false} {
}

void pinned_allocator::setUcpContext(ucp_context_h _context)
    {
    context = _context;
    use_ucx = true;
    }

void base_allocator::allocate(void ** ptr, std::size_t size, ucp_mem_h * mem_handle_ptr){
  do_allocate(ptr,size,mem_handle_ptr);
}

void base_allocator::deallocate(void * ptr, ucp_mem_h mem_handle){
  do_deallocate(ptr,mem_handle);
}

void host_allocator::do_allocate(void ** ptr, std::size_t size, ucp_mem_h * mem_handle_ptr){
  
  *ptr = aligned_alloc( BLAZING_ALIGNMENT, size );
  if (!ptr) {
    throw std::runtime_error("Couldn't perform host allocation.");
  }
}

void pinned_allocator::do_allocate(void ** ptr, std::size_t size, ucp_mem_h * mem_handle_ptr){

  // do we really want to do a host allocation instead of a device one? (have to try zero-copy later)
  cudaError_t err = cudaMallocHost(ptr, size);
  if (err != cudaSuccess) {
    throw std::runtime_error("Couldn't perform pinned allocation.");
  }

  if (use_ucx) {
    ucp_mem_map_params_t mem_map_params;
    std::memset(&mem_map_params, 0, sizeof(ucp_mem_map_params_t));
    mem_map_params.field_mask = UCP_MEM_MAP_PARAM_FIELD_ADDRESS |
                                    UCP_MEM_MAP_PARAM_FIELD_LENGTH |
                                    UCP_MEM_MAP_PARAM_FIELD_FLAGS;
    mem_map_params.address = *ptr;
    mem_map_params.length = size;
    mem_map_params.flags = 0; // try UCP_MEM_MAP_NONBLOCK

    ucs_status_t status = ucp_mem_map(context, &mem_map_params, mem_handle_ptr);
    if (status != UCS_OK)
        {
        throw std::runtime_error("Error on ucp_mem_map");
        }
  }
}

void host_allocator::do_deallocate(void * ptr, ucp_mem_h mem_handle){
  free(ptr);
}

void pinned_allocator::do_deallocate(void * ptr, ucp_mem_h mem_handle){
  if (use_ucx)
     {
     ucs_status_t status = ucp_mem_unmap(context, mem_handle);
     if (status != UCS_OK)
        {
        throw std::runtime_error("Error on ucp_mem_map");
        }
    }
  auto err = cudaFreeHost(ptr);
  if (err != cudaSuccess) {
    throw std::runtime_error("Couldn't free pinned allocation.");
  }
}


allocation_pool::allocation_pool(std::unique_ptr<base_allocator> allocator, std::size_t size_buffers, std::size_t num_buffers) :
num_buffers (num_buffers), buffer_size(size_buffers), allocator(std::move(allocator)) {
  this->buffer_counter = 0; // this will get incremented by grow()
  this->grow();
  
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
        auto temp = std::move(allocation->allocation_chunks.top());
        allocation->allocation_chunks.pop();
        
        return std::move(temp);
    }
  }
  
  //TODO: make exception for this
  throw std::runtime_error("Blazing allocation pool failed to grow or allocate.");
}



void allocation_pool::grow() {
  // if this is the first growth (initializaton) then we want num_buffers, else we will just grow by half that.
  std::size_t num_new_buffers = this->buffer_counter == 0 ? this->num_buffers : this->num_buffers/2;
  allocations.push_back(std::make_unique<blazing_allocation>());
  allocations.back()->index = this->allocations.size() - 1;
  auto last_index = allocations.size() -1;
  try{
    allocator->allocate((void **) &allocations[last_index]->data,num_new_buffers * buffer_size, &allocations[last_index]->mem_handle);
    this->allocations[last_index]->total_number_of_chunks = num_new_buffers;
    for (int buffer_index = 0; buffer_index < num_new_buffers; buffer_index++) {
       auto buffer = std::make_unique<blazing_allocation_chunk>();
      buffer->size = this->buffer_size;
      buffer->data = allocations[last_index]->data + buffer_index * this->buffer_size;
      buffer->allocation = allocations[last_index].get();
      this->allocations[last_index]->allocation_chunks.push(std::move(buffer));
      this->buffer_counter++;
    }
    allocations[last_index]->size = num_new_buffers * buffer_size;
    allocations[last_index]->pool = this;
  }catch(std::exception & e){
    throw;
  }
}

void allocation_pool::free_chunk(std::unique_ptr<blazing_allocation_chunk> buffer) {
  std::unique_lock<std::mutex> lock(in_use_mutex);
  const std::size_t idx = buffer->allocation->index;

  if (idx >= this->allocations.size()) {
    std::shared_ptr<spdlog::logger> logger = spdlog::get("batch_logger");
    if(logger){
      logger->error("|||{0}|||||","free_chunk cannot delete an invalid allocation.");
    }
    assert(("free_chunk cannot delete an invalid allocation.", idx < this->allocations.size()));
  }

  this->allocations.at(idx)->allocation_chunks.push(std::move(buffer));

  if (idx > 0) {
    if (this->allocations.at(idx)->total_number_of_chunks == this->allocations.at(idx)->allocation_chunks.size()) {
      if (this->allocations.at(idx)->data != nullptr) {
        this->buffer_counter -= this->allocations.at(idx)->total_number_of_chunks;
        this->allocator->deallocate(this->allocations.at(idx)->data, this->allocations.at(idx)->mem_handle);
                
        // for all allocations after the pos at idx
        // we need to update the allocation.index after we deleted one
        for (std::size_t i = idx; i < this->allocations.size(); ++i) {
          this->allocations[i]->index = this->allocations[i]->index - 1;
        }

        this->allocations.erase(this->allocations.begin() + idx);
      }
    }
  }  
}


void allocation_pool::free_all() {
  std::unique_lock<std::mutex> lock(in_use_mutex);
  if (this->buffer_counter > 0){
    this->buffer_counter = 0;
    for(auto & allocation : allocations){
      while (false == allocation->allocation_chunks.empty()) {
        auto buffer = std::move(allocation->allocation_chunks.top());
        allocation->allocation_chunks.pop();
      }
      allocator->deallocate(allocation->data, allocation->mem_handle);
    }
    allocations.resize(0);    
  }
}

std::size_t allocation_pool::size_buffers() { return this->buffer_size; }


void set_allocation_pools(std::size_t size_buffers_host, std::size_t num_buffers_host,
std::size_t size_buffers_pinned, std::size_t num_buffers_pinned, bool map_ucx,
    ucp_context_h context) {

  if (buffer_providers::get_host_buffer_provider() == nullptr || buffer_providers::get_host_buffer_provider()->get_total_buffers() == 0) { // not initialized

    auto host_alloc = std::make_unique<host_allocator>(false);

    buffer_providers::get_host_buffer_provider() = std::make_shared<allocation_pool>(
    std::move(host_alloc) ,size_buffers_host,num_buffers_host);
  }

  if (buffer_providers::get_pinned_buffer_provider() == nullptr || buffer_providers::get_pinned_buffer_provider()->get_total_buffers() == 0) { // not initialized
    auto pinned_alloc = std::make_unique<pinned_allocator>();

    if (map_ucx) {
      pinned_alloc->setUcpContext(context);
    }

    buffer_providers::get_pinned_buffer_provider() = std::make_shared<allocation_pool>(std::move(pinned_alloc),
      size_buffers_host,num_buffers_host);
  }
}

void empty_pools(){
  buffer_providers::get_host_buffer_provider()->free_all();
  buffer_providers::get_pinned_buffer_provider()->free_all();
}

std::size_t allocation_pool::get_total_buffers(){
  return buffer_counter;
}


std::pair< std::vector<ral::memory::blazing_chunked_column_info>, std::vector<std::unique_ptr<ral::memory::blazing_allocation_chunk> >> convert_gpu_buffers_to_chunks(
  std::vector<std::size_t> buffer_sizes,bool use_pinned){

  
  size_t buffer_index = 0;
  size_t allocation_position = 0;

  std::shared_ptr<allocation_pool > pool = use_pinned ? buffer_providers::get_pinned_buffer_provider() : buffer_providers::get_host_buffer_provider();
  
  std::vector<std::unique_ptr<ral::memory::blazing_allocation_chunk> > allocations;
  std::vector<ral::memory::blazing_chunked_column_info> chunked_column_infos; 
  std::unique_ptr<ral::memory::blazing_allocation_chunk> current_allocation = pool->get_chunk();
  
  while(buffer_index < buffer_sizes.size()){
    ral::memory::blazing_chunked_column_info chunked_column_info;
    chunked_column_info.use_size = buffer_sizes[buffer_index];
    size_t buffer_position = 0;
    while(buffer_position < chunked_column_info.use_size){

      if(allocation_position == current_allocation->size){
        allocation_position = 0;
        allocations.push_back(std::move(current_allocation));
        current_allocation = pool->get_chunk();
      }
      size_t chunk_index = allocations.size();
      size_t offset = allocation_position;
      size_t size;
      //if the number of bytes left to write fits in the current allocation
      if((chunked_column_info.use_size - buffer_position) <= (current_allocation->size - allocation_position)){
        size = chunked_column_info.use_size - buffer_position;
        allocation_position += size;
        buffer_position += size;

      }else {
        size = current_allocation->size - allocation_position;
        buffer_position += size;
        allocation_position += size;
      }
      chunked_column_info.chunk_index.push_back(chunk_index);
      chunked_column_info.offset.push_back(offset);
      chunked_column_info.size.push_back(size);
    }
    buffer_index++;
    chunked_column_infos.push_back(chunked_column_info);
  }  
  //add the last allocation to the list
  allocations.push_back(std::move(current_allocation));

  return std::make_pair< std::vector<ral::memory::blazing_chunked_column_info>, std::vector<std::unique_ptr<ral::memory::blazing_allocation_chunk> >> (std::move(chunked_column_infos), std::move(allocations));
}

}
}
