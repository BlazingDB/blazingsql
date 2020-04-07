#pragma once 

#include <cassert>
#include <atomic>

#include <cuda_runtime_api.h>

#include <rmm/rmm_api.h>
#include <rmm/detail/memory_manager.hpp>
#include <rmm/mr/device/device_memory_resource.hpp>
#include <rmm/mr/device/cuda_memory_resource.hpp>
#include <rmm/mr/device/managed_memory_resource.hpp>
#include <rmm/mr/device/cnmem_managed_memory_resource.hpp>
#include <rmm/mr/device/cnmem_memory_resource.hpp>

#include "config/GPUManager.cuh"

#include <sys/sysinfo.h>
#include <sys/statvfs.h>

class BlazingMemoryResource {
	virtual std::size_t get_used_memory_size() = 0 ;
	virtual std::size_t get_total_memory_size() = 0 ;
};

class blazing_device_memory_resource : public rmm::mr::device_memory_resource, BlazingMemoryResource { 
public:
	blazing_device_memory_resource(rmmOptions_t rmmValues) {
		total_memory_size = ral::config::gpuMemorySize();
		used_memory = 0;

		rmmAllocationMode_t allocation_mode = static_cast<rmmAllocationMode_t>(rmmValues.allocation_mode);
		if (allocation_mode == (CudaManagedMemory | PoolAllocation) || allocation_mode == PoolAllocation) {
			if (total_memory_size <= rmmValues.initial_pool_size) {
				throw std::runtime_error("Cannot allocate this Pool memory size on the GPU.");
			} 

			if (allocation_mode == CudaManagedMemory | PoolAllocation) {
				memory_resource = std::make_unique<rmm::mr::cnmem_managed_memory_resource>();
			} else {
				memory_resource = std::make_unique<rmm::mr::cnmem_memory_resource>();
			}

			used_memory += rmmValues.initial_pool_size;
		} 
		else if (allocation_mode == CudaManagedMemory) {
			memory_resource = std::make_unique<rmm::mr::managed_memory_resource>();
		}
		else { // allocation_mode == useCudaDefaultAllocator
			memory_resource = std::make_unique<rmm::mr::cuda_memory_resource>();
		}
	}

	virtual ~blazing_device_memory_resource() = default;

	std::size_t get_used_memory_size() {
		return used_memory;
	}

	std::size_t get_total_memory_size() {
		return total_memory_size;
	}

	bool supports_streams() const noexcept override { return false; }
	bool supports_get_mem_info() const noexcept override { return true; }

private:
	void* do_allocate(std::size_t bytes, cudaStream_t stream) override {
		if (bytes <= 0) return nullptr;
		if (total_memory_size <= used_memory + bytes) {
			throw std::runtime_error("Cannot allocate this size memory on the GPU.");
		}
		used_memory += bytes;

		return memory_resource->allocate(bytes, stream);
	}

	void do_deallocate(void* p, std::size_t bytes, cudaStream_t stream) override {
		if (nullptr == p || bytes == 0) return;
		if (used_memory < bytes) {
			std::cerr << "Deallocating more bytes than used right now, used_memory: " << used_memory << " less than " << bytes << " bytes." << std::endl;
			used_memory = 0;
		} else {
			used_memory -= bytes;
		}

		return memory_resource->deallocate(p, bytes, stream);
	}

	bool do_is_equal(device_memory_resource const& other) const noexcept override {
		return dynamic_cast<blazing_device_memory_resource const*>(&other) != nullptr;
	}

	std::pair<size_t, size_t> do_get_mem_info(cudaStream_t) const override {
		std::size_t free_size;
		std::size_t total_size;
		return std::make_pair(free_size, total_size);
	}

	std::size_t total_memory_size;
	std::atomic<std::size_t> used_memory;
	std::unique_ptr<rmm::mr::device_memory_resource> memory_resource;
};

class blazing_host_memory_mesource : BlazingMemoryResource{
public:
	// TODO: percy,cordova. Improve the design of get memory in real time 
	blazing_host_memory_mesource() {
		struct sysinfo si;
		sysinfo (&si);

		total_memory_size = (std::size_t)si.totalram;
		used_memory_size =  total_memory_size - (std::size_t)si.freeram;;
	}

	virtual ~blazing_host_memory_mesource() = default;

	std::size_t get_used_memory_size() override {
		return used_memory_size;
	}

	std::size_t get_total_memory_size() override {
		return total_memory_size;
	}

private:
	std::size_t total_memory_size;
	std::atomic<std::size_t> used_memory_size;
};


class blazing_disk_memory_resource : BlazingMemoryResource {
public:
	// TODO: percy, cordova.Improve the design of get memory in real time 
	blazing_disk_memory_resource() {
		struct statvfs stat_disk;
		int ret = statvfs("/", &stat_disk);

		total_memory_size = (std::size_t)(stat_disk.f_blocks * stat_disk.f_frsize);
		std::size_t available_disk_size = (std::size_t)(stat_disk.f_bfree * stat_disk.f_frsize);
		used_memory_size = total_memory_size - available_disk_size;
	}

	virtual ~blazing_disk_memory_resource() = default;

	std::size_t get_used_memory_size() {
		return used_memory_size;
	}

	std::size_t get_total_memory_size() {
		return total_memory_size;
	}

private:
	std::size_t total_memory_size;
	std::atomic<std::size_t> used_memory_size;
};
