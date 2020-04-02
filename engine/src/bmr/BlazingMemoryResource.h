#pragma once 

#include <cassert>
#include <atomic>

#include <cuda_runtime_api.h>

#include <rmm/device_memory_resource.hpp>

#include <sys/sysinfo.h>
#include <sys/statvfs.h>

class BlazingMemoryResource {
  virtual size_t get_used_memory_size() = 0 ;
  virtual size_t get_total_memory_size() = 0 ;
};

// /TODO copy cuda_memory_resource implemenation and add counters
class blazing_device_memory_resource : public device_memory_resource, BlazingMemoryResource {
  public:
    blazing_device_memory_resource() = default;
    virtual ~blazing_device_memory_resource() = default;

  private:
    void* do_allocate(std::size_t bytes, cudaStream_t stream) {
      if (total_memory_size < used_memory + bytes) {
        throw std::runtime_error("Cannot allocate more memory on the GPU.");
      }
      if (bytes <= 0) return nullptr;
      used_memory += bytes;

      void* p{nullptr};
      CUDA_TRY(cudaMalloc(&p, bytes));
      return p;
    }

    void do_deallocate(void* p, std::size_t bytes, cudaStream_t stream) {
      if (nullptr == p) return;
      if (used_memory < bytes) {
        used_memory = 0;
      } else {
        used_memory -= bytes;
      }

      cudaError_t const status = cudaFree(p);
      assert(cudaSuccess == status);
    }


    size_t get_used_memory_size();
    size_t get_total_memory_size();

  private:
    size_t total_memory_size;       // POOL_SIZE, GPU_DRIVER_MEM_INFO, .... 
    std::atomic<size_t> used_memory{0};    
};


class blazing_host_memory_mesource : BlazingMemoryResource{
  public:
    blazing_host_memory_mesource() = default;
    virtual ~blazing_host_memory_mesource() = default;

    // Compute the total RAM size in bytes
    size_t get_total_memory_size() {
      struct sysinfo si;
      sysinfo (&si);

      size_t total_ram = si.totalram;
      return total_ram;
    }

    //Compute the used RAM size in bytes
    size_t get_used_memory_size() {
      struct sysinfo si;
      sysinfo (&si);

      size_t total_ram = si.totalram;
      size_t free_ram = si.freeram
      return total_ram - free_ram;
    }
};


class blazing_disk_memory_resource : BlazingMemoryResource {
  public:
    blazing_disk_memory_resource() = default;
    virtual ~blazing_disk_memory_resource() = default;

    // TODO: cordova change the actual current_path
    size_t get_total_memory_size(std::string current_path = "/home/") {
      struct statvfs stat_disk;
      int ret = statvfs(current_path, &stat_disk);
      size_t total_disk_size = (size_t)(stat_disk.f_blocks * stat_disk.f_frsize);

      return total_disk_size;
  }

    // TODO: cordova change the actual current_path
    size_t get_used_memory_size(std::string current_path = "/home/") {
    
      struct statvfs stat_disk;
      int ret = statvfs(current_path, &stat_disk);
      size_t total_disk_size = (size_t)(stat_disk.f_blocks * stat_disk.f_frsize);
      size_t available_disk_size = (size_t)(stat_disk.f_bfree * stat_disk.f_frsize);
      size_t used_disk_size = total_disk_size - available_disk_size;

      return used_disk_size;
    }
};

