#pragma once 


class BlazingMemoryResource {
  virtual size_t get_used_memory() = 0 ;
  virtual size_t get_total_memory_size() = 0 ;
};

// /TODO copy cuda_memory_resource implemenation and add counters
class BlazingDeviceMemoryResource  : public device_memory_resource, BlazingMemoryResource {
 public:
  blazing_memory_resource() = default;
  virtual ~blazing_memory_resource() = default;
  // virtual 
   virtual void* do_allocate(std::size_t bytes, cudaStream_t stream) {
    // TRY 
    if (total_memory_size <= used_memory + bytes){
      used_memory += bytes;
      return this->do_allocate(bytes, stream);
    }
    // THROW 
  }
  // virtual 
  virtual void do_deallocate(void* p, std::size_t bytes, cudaStream_t stream){
    used_memory -= bytes;
    return this->do_deallocate(p, bytes, stream);
  }
  virtual size_t get_used_memory() = 0 ;
  virtual size_t get_total_memory_size() = 0 ;
private:
  size_t total_memory_size; // POOL_SIZE, GPU_DRIVER_MEM_INFO, .... 
  std::atomic<size_t> used_memory{0};    
};


class BlazingHostMemoryResource : BlazingMemoryResource{
  // TODO: api RAM TOTAL_MEMORY_SIZE(); kernel linux

  virtual size_t get_used_memory() = 0 ;
  virtual size_t get_total_memory_size() = 0 ;
};

class BlazingDiskMemoryResource : BlazingMemoryResource {
  // TODO: api DISK TOTAL_MEMORY_SIZE(); kernel linux

  virtual size_t get_used_memory() = 0 ;
  virtual size_t get_total_memory_size() = 0 ;
};

