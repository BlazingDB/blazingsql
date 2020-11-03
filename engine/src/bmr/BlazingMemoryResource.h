#pragma once 

#include <cassert>
#include <atomic>
#include <set>

#include <cuda_runtime_api.h>
#include <rmm/cuda_stream_view.hpp>
#include <rmm/mr/device/device_memory_resource.hpp>


#include <rmm/mr/device/owning_wrapper.hpp>
#include <rmm/mr/device/cuda_memory_resource.hpp>
#include <rmm/mr/device/managed_memory_resource.hpp>
#include <rmm/mr/device/pool_memory_resource.hpp>
#include <rmm/mr/device/logging_resource_adaptor.hpp>
#include <rmm/mr/device/per_device_resource.hpp>

#include "config/GPUManager.cuh"

#include <sys/sysinfo.h>
#include <sys/statvfs.h>

/**
	@brief This interface represents a custom memory resource used in the cache system.
    The Cache Machines uses singleton references to device, host and disk memory resources. 
    Each object of the CacheMachine class has knownlegde about the status of the memory resource by using
    `get_memory_limit`  and `get_memory_used` methods.
*/
class BlazingMemoryResource {
public:
	virtual size_t get_from_driver_available_memory() = 0 ; // driver.get_available_memory()
	virtual size_t get_memory_limit() = 0 ; // memory_limite = total_memory * threshold

	virtual size_t get_memory_used() = 0 ; // atomic 
	virtual size_t get_total_memory() = 0 ; // total_memory
};

/**
	@brief This class represents the internal implementation of a custom device  memory resource.
*/
class internal_blazing_device_memory_resource : public rmm::mr::device_memory_resource { 
public:
    // TODO: use another constructor for memory in bytes

	internal_blazing_device_memory_resource(std::string allocation_mode,
                                            std::size_t initial_pool_size,
                                            std::size_t maximum_pool_size,
                                            std::string allocator_logging_file = "",
                                            float custom_threshold = 0.95)
    {
		total_memory_size = ral::config::gpuTotalMemory();
		used_memory = 0;
        memory_limit = (double)custom_threshold * total_memory_size;

        initial_pool_size = initial_pool_size - initial_pool_size % 256; //initial_pool_size required to be a multiple of 256 bytes 

        if (total_memory_size <= initial_pool_size) {
            throw std::runtime_error("Cannot allocate this Pool memory size on the GPU.");
        }

        if (allocation_mode == "cuda_memory_resource"){
            memory_resource_owner = std::make_shared<rmm::mr::cuda_memory_resource>();
            memory_resource = memory_resource_owner.get();
        } else if (allocation_mode == "managed_memory_resource"){
            memory_resource_owner = std::make_shared<rmm::mr::managed_memory_resource>();
            memory_resource = memory_resource_owner.get();
        } else if (allocation_mode == "pool_memory_resource") {
            memory_resource_owner = rmm::mr::make_owning_wrapper<rmm::mr::pool_memory_resource>(
                std::make_shared<rmm::mr::cuda_memory_resource>(), initial_pool_size, maximum_pool_size);
            memory_resource = memory_resource_owner.get();
        } else if (allocation_mode == "managed_pool_memory_resource") {
            memory_resource_owner = rmm::mr::make_owning_wrapper<rmm::mr::pool_memory_resource>(
                std::make_shared<rmm::mr::managed_memory_resource>(), initial_pool_size, maximum_pool_size);
            memory_resource = memory_resource_owner.get();
        } else if (allocation_mode == "existing"){
            memory_resource = rmm::mr::get_current_device_resource();        
        } else {
            throw std::runtime_error("ERROR creating internal_blazing_device_memory_resource: allocation_mode not recognized.");
        }
        type = allocation_mode;

        if (allocator_logging_file != ""){
            logging_adaptor.reset(new rmm::mr::logging_resource_adaptor<rmm::mr::device_memory_resource>(
                memory_resource, allocator_logging_file, /*auto_flush=*/true));
            memory_resource = logging_adaptor.get();
        }
	}

	virtual ~internal_blazing_device_memory_resource() = default;

	size_t get_memory_used() {
		return used_memory;
	}
    size_t get_from_driver_available_memory() {
	    return ral::config::gpuUsedMemory();
    }

	size_t get_total_memory() {
		return total_memory_size;
	}
	size_t get_memory_limit() {
        return memory_limit;
    }

    std::string get_type() {
		return type;
    }

	bool supports_streams() const noexcept override { return memory_resource->supports_streams(); }
	bool supports_get_mem_info() const noexcept override { return memory_resource->supports_get_mem_info(); }

private: 
	void* do_allocate(size_t bytes, rmm::cuda_stream_view stream) override {
		if (bytes <= 0) { 
            return nullptr;
		}
		used_memory += bytes;
		return memory_resource->allocate(bytes, stream);
	}

	void do_deallocate(void* p, size_t bytes, rmm::cuda_stream_view stream) override {
		if (nullptr == p || bytes == 0) return;
		if (used_memory < bytes) {
			std::cerr << "blazing_device_memory_resource: Deallocating more bytes than used right now, used_memory: " << used_memory << " less than " << bytes << " bytes." << std::endl;
			used_memory = 0;
		} else {
			used_memory -= bytes;
		}

		return memory_resource->deallocate(p, bytes, stream);
	}

	bool do_is_equal(device_memory_resource const& other) const noexcept override {
		return memory_resource->is_equal(other);
	}

	std::pair<size_t, size_t> do_get_mem_info(rmm::cuda_stream_view stream) const override {
		return memory_resource->get_mem_info(stream);
	}

	size_t total_memory_size;
	size_t memory_limit;
	std::atomic<size_t> used_memory;
    std::shared_ptr<rmm::mr::device_memory_resource> memory_resource_owner;
    rmm::mr::device_memory_resource * memory_resource;
    std::unique_ptr<rmm::mr::logging_resource_adaptor<rmm::mr::device_memory_resource>> logging_adaptor;
    std::string type;
};

// forward declaration
typedef struct CUstream_st *cudaStream_t;

/** -------------------------------------------------------------------------*
 * @brief RMM blazing_device_memory_resource class maintains the device memory manager context, including
 * the RMM event log, configuration options, and registered streams.
 * 
 * blazing_device_memory_resource is a singleton class, and should be accessed via getInstance(). 
 * A number of static convenience methods are provided that wrap getInstance()
 * ------------------------------------------------------------------------**/
class blazing_device_memory_resource : public BlazingMemoryResource {
public:
    /** -----------------------------------------------------------------------*
     * @brief Get the blazing_device_memory_resource instance singleton object
     * 
     * @return blazing_device_memory_resource& the blazing_device_memory_resource singleton
     * ----------------------------------------------------------------------**/
    static blazing_device_memory_resource& getInstance(){
        // Myers' singleton. Thread safe and unique. Note: C++11 required.
        static blazing_device_memory_resource instance;
        return instance;
    }

	size_t get_memory_used() {
		return initialized_resource->get_memory_used();
	}

	size_t get_total_memory() {
		return initialized_resource->get_total_memory() ;
	}

    size_t get_from_driver_available_memory()  {
        return initialized_resource->get_from_driver_available_memory();
    }
	size_t get_memory_limit() {
		return initialized_resource->get_memory_limit() ;
    }

    std::string get_type() {
		return initialized_resource->get_type() ;
    }

  /** -----------------------------------------------------------------------*
   * @brief Initialize RMM options
   * 
   *   allocator          :  "managed" or "default" or "existing", where "managed" uses Unified Virtual Memory (UVM)
   *                            and may use system memory if GPU memory runs out, "default" uses the default Cuda allocation
   *                            and "existing" assumes rmm allocator is already set and does not initialize it.
   *                            "managed" is the BlazingSQL default, since it provides the most robustness against OOM errors.
   *   pool               : if True, BlazingContext will self-allocate a GPU memory pool. can greatly improve performance.
   *   initial_pool_size  : initial size of memory pool in bytes (if pool=True).
   *                                   if None, and pool=True, defaults to 1/2 GPU memory.
   *   maximum_pool_size  : maximum size of memory pool in bytes (if pool=True).
   *                                   if None, and pool=True, defaults to all the GPU memory.
   *   allocator_logging_file : File that would be used by the allocator logger. If empty, then no allocator logging will be enabled.
   *   device_mem_resouce_consumption_thresh : The percent (as a decimal) of total GPU memory that the memory resource
   * 
   * @param[in] options Optional options to set
   * ----------------------------------------------------------------------**/
    void initialize(std::string allocation_mode,
                    std::size_t initial_pool_size,
                    std::size_t maximum_pool_size,
                    std::string allocator_logging_file,
                    float device_mem_resouce_consumption_thresh) {
        
        std::lock_guard<std::mutex> guard(manager_mutex);

        // repeat initialization is a no-op
        if (isInitialized()) return;

        initialized_resource.reset(new internal_blazing_device_memory_resource(
                allocation_mode, initial_pool_size, maximum_pool_size, 
                allocator_logging_file, device_mem_resouce_consumption_thresh));
        
        rmm::mr::set_current_device_resource(initialized_resource.get());
        
        is_initialized = true;
    }

    /** -----------------------------------------------------------------------*
     * @brief Shut down the blazing_device_memory_resource (clears the context)
     * ----------------------------------------------------------------------**/
    void finalize(){
        std::lock_guard<std::mutex> guard(manager_mutex);

        // finalization before initialization is a no-op
        if (isInitialized()) {
            registered_streams.clear();
            initialized_resource.reset();
            is_initialized = false;
        }
    }

    /** -----------------------------------------------------------------------*
     * @brief Check whether the blazing_device_memory_resource has been initialized.
     * 
     * @return true if blazing_device_memory_resource has been initialized.
     * @return false if blazing_device_memory_resource has not been initialized.
     * ----------------------------------------------------------------------**/
    bool isInitialized() {
        return getInstance().is_initialized;
    }
   
   
private:
    blazing_device_memory_resource() = default;
    ~blazing_device_memory_resource() = default;
    blazing_device_memory_resource(const blazing_device_memory_resource&) = delete;
    blazing_device_memory_resource& operator=(const blazing_device_memory_resource&) = delete;
    std::mutex manager_mutex;
    std::set<cudaStream_t> registered_streams;

    bool is_initialized{false};

    std::unique_ptr<internal_blazing_device_memory_resource> initialized_resource{};
};

/**
	@brief This class represents a custom host memory resource used in the cache system.
*/
class internal_blazing_host_memory_resource{
public:
	// TODO: percy,cordova. Improve the design of get memory in real time 
	internal_blazing_host_memory_resource(float custom_threshold) 
    {
		struct sysinfo si;
		if (sysinfo(&si) < 0) {
            std::cerr << "@@ error sysinfo host "<< std::endl;
        } 
        total_memory_size = (size_t)si.freeram;
        used_memory_size = 0;
        memory_limit = custom_threshold * total_memory_size;
	}

	virtual ~internal_blazing_host_memory_resource() = default;

    // TODO
    void allocate(std::size_t bytes)  {
		used_memory_size +=  bytes;
	}

	void deallocate(std::size_t bytes)  {
		used_memory_size -= bytes;
	}

	size_t get_from_driver_available_memory()  {
        struct sysinfo si;
		sysinfo (&si);
        // NOTE: sync point 
		total_memory_size = (size_t)si.totalram;
		used_memory_size = total_memory_size - (size_t)si.freeram;;
        return used_memory_size;
    }

	size_t get_memory_used() {
		return used_memory_size;
	}

	size_t get_total_memory() {
		return total_memory_size;
	}

    size_t get_memory_limit() {
        return memory_limit;
    }

private:
    size_t memory_limit;
	size_t total_memory_size;
	std::atomic<std::size_t> used_memory_size;
};

/** -------------------------------------------------------------------------*
 * @brief blazing_host_memory_resource class maintains the host memory manager context.
 * 
 * blazing_host_memory_resource is a singleton class, and should be accessed via getInstance(). 
 * A number of static convenience methods are provided that wrap getInstance()..
 * ------------------------------------------------------------------------**/
class blazing_host_memory_resource : public BlazingMemoryResource {
public:
    /** -----------------------------------------------------------------------*
     * @brief Get the blazing_host_memory_resource instance singleton object
     * 
     * @return blazing_host_memory_resource& the blazing_host_memory_resource singleton
     * ----------------------------------------------------------------------**/
    static blazing_host_memory_resource& getInstance(){
        // Myers' singleton. Thread safe and unique. Note: C++11 required.
        static blazing_host_memory_resource instance;
        return instance;
    }

	size_t get_memory_used() override {
		// std::cout << "blazing_host_memory_resource: " << initialized_resource->get_memory_used() << std::endl; 
		return initialized_resource->get_memory_used();
	}

	size_t get_total_memory() override {
		return initialized_resource->get_total_memory() ;
	}

    size_t get_from_driver_available_memory()  {
        return initialized_resource->get_from_driver_available_memory();
    }
	size_t get_memory_limit() {
		return initialized_resource->get_memory_limit() ;
    }

    void allocate(std::size_t bytes)  {
		initialized_resource->allocate(bytes);
	}

	void deallocate(std::size_t bytes)  {
		initialized_resource->deallocate(bytes);
	}

   /** -----------------------------------------------------------------------*
   * @brief Initialize
   * 
   * Accepts an optional rmmOptions_t struct that describes the settings used
   * to initialize the memory manager. If no `options` is passed, default
   * options are used.
   * 
   * @param[in] options Optional options to set
   * ----------------------------------------------------------------------**/
    void initialize(float host_mem_resouce_consumption_thresh) {
        
        std::lock_guard<std::mutex> guard(manager_mutex);

        // repeat initialization is a no-op
        if (isInitialized()) return;

        initialized_resource.reset(new internal_blazing_host_memory_resource(host_mem_resouce_consumption_thresh));

        is_initialized = true;
    }

     /** -----------------------------------------------------------------------*
     * @brief Shut down the blazing_device_memory_resource (clears the context)
     * ----------------------------------------------------------------------**/
    void finalize(){
        std::lock_guard<std::mutex> guard(manager_mutex);

        // finalization before initialization is a no-op
        if (isInitialized()) {
            initialized_resource.reset();
            is_initialized = false;
        }
    }

    /** -----------------------------------------------------------------------*
     * @brief Check whether the blazing_device_memory_resource has been initialized.
     * 
     * @return true if blazing_device_memory_resource has been initialized.
     * @return false if blazing_device_memory_resource has not been initialized.
     * ----------------------------------------------------------------------**/
    bool isInitialized() {
        return getInstance().is_initialized;
    }

private:
    blazing_host_memory_resource() = default;
    ~blazing_host_memory_resource() = default;
    blazing_host_memory_resource(const blazing_host_memory_resource&) = delete;
    blazing_host_memory_resource& operator=(const blazing_host_memory_resource&) = delete;
    std::mutex manager_mutex;

    bool is_initialized{false};

    std::unique_ptr<internal_blazing_host_memory_resource> initialized_resource{};
};

/**
	@brief This class represents a custom disk memory resource used in the cache system.
*/
class blazing_disk_memory_resource : public  BlazingMemoryResource {
public:
    static blazing_disk_memory_resource& getInstance(){
        // Myers' singleton. Thread safe and unique. Note: C++11 required.
        static blazing_disk_memory_resource instance;
        return instance;
    }

	// TODO: percy, cordova.Improve the design of get memory in real time 
	blazing_disk_memory_resource(float custom_threshold = 0.75) {
		struct statvfs stat_disk;
		int ret = statvfs("/", &stat_disk);

		total_memory_size = (size_t)(stat_disk.f_blocks * stat_disk.f_frsize);
		size_t available_disk_size = (size_t)(stat_disk.f_bfree * stat_disk.f_frsize);
		used_memory_size = total_memory_size - available_disk_size;

        memory_limit = custom_threshold *  total_memory_size;
	}

	virtual ~blazing_disk_memory_resource() = default;

	virtual size_t get_from_driver_available_memory()  {
        struct sysinfo si;
        sysinfo (&si);
        // NOTE: sync point 
        total_memory_size = (size_t)si.totalram;
        used_memory_size =  total_memory_size - (size_t)si.freeram;
        return used_memory_size;
    }
	size_t get_memory_limit()  {
        return memory_limit;
    }

	size_t get_memory_used() {
        return used_memory_size;
	}

	size_t get_total_memory() {
		return total_memory_size;
	}

private:
	size_t total_memory_size;
    size_t memory_limit;
	std::atomic<size_t> used_memory_size;
};
