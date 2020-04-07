#ifndef BLAZING_MEMORY_MANAGER_H
#define BLAZING_MEMORY_MANAGER_H

#include <vector>
#include <chrono>
#include <ctime>
#include <iomanip>
#include <iostream>
#include <set>
#include <mutex>

#include <bmr/BlazingMemoryResource.h>
#include <rmm/mr/device/device_memory_resource.hpp>

// forward declaration
typedef struct CUstream_st *cudaStream_t;

/** -------------------------------------------------------------------------*
 * @brief RMM BlazingMemoryManager class maintains the memory manager context, including
 * the RMM event log, configuration options, and registered streams.
 * 
 * BlazingMemoryManager is a singleton class, and should be accessed via getInstance(). 
 * A number of static convenience methods are provided that wrap getInstance(),
 * such as getLogger() and getOptions().
 * ------------------------------------------------------------------------**/
class BlazingMemoryManager{
public:
    /** -----------------------------------------------------------------------*
     * @brief Get the BlazingMemoryManager instance singleton object
     * 
     * @return BlazingMemoryManager& the BlazingMemoryManager singleton
     * ----------------------------------------------------------------------**/
    static BlazingMemoryManager& getInstance(){
        // Myers' singleton. Thread safe and unique. Note: C++11 required.
        static BlazingMemoryManager instance;
        return instance;
    }

  /** -----------------------------------------------------------------------*
   * @brief Initialize RMM options
   * 
   * Accepts an optional rmmOptions_t struct that describes the settings used
   * to initialize the memory manager. If no `options` is passed, default
   * options are used.
   * 
   * @param[in] options Optional options to set
   * ----------------------------------------------------------------------**/
    void initialize(const rmmOptions_t *new_options) {
        
        std::lock_guard<std::mutex> guard(manager_mutex);

        // repeat initialization is a no-op
        if (isInitialized()) return;

        if (nullptr != new_options) options = *new_options;

        initialized_resource.reset(new blazing_device_memory_resource(options));
        
        rmm::mr::set_default_resource(initialized_resource.get());
        
        is_initialized = true;
    }

    /** -----------------------------------------------------------------------*
     * @brief Shut down the BlazingMemoryManager (clears the context)
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
     * @brief Check whether the BlazingMemoryManager has been initialized.
     * 
     * @return true if BlazingMemoryManager has been initialized.
     * @return false if BlazingMemoryManager has not been initialized.
     * ----------------------------------------------------------------------**/
    bool isInitialized() {
        return getInstance().is_initialized;
    }

    /** -----------------------------------------------------------------------*
     * @brief Get the Options object
     * 
     * @return rmmOptions_t the currently set RMM options
     * ----------------------------------------------------------------------**/
    static rmmOptions_t getOptions() { return getInstance().options; }

    /** -----------------------------------------------------------------------*
     * @brief Returns true when pool allocation is enabled
     * 
     * @return true if pool allocation is enabled
     * @return false if pool allocation is disabled
     * ----------------------------------------------------------------------**/
    static inline bool usePoolAllocator() {
        return getOptions().allocation_mode & PoolAllocation;
    }

    /** -----------------------------------------------------------------------*
     * @brief Returns true if CUDA Managed Memory allocation is enabled
     * 
     * @return true if CUDA Managed Memory allocation is enabled
     * @return false if CUDA Managed Memory allocation is disabled
     * ----------------------------------------------------------------------**/
    static inline bool useManagedMemory() {
        return getOptions().allocation_mode & CudaManagedMemory;
    }

    /** -----------------------------------------------------------------------*
     * @brief Returns true when CUDA default allocation is enabled
     *          * 
     * @return true if CUDA default allocation is enabled
     * @return false if CUDA default allocation is disabled
     * ----------------------------------------------------------------------**/
    inline bool useCudaDefaultAllocator() {
        return CudaDefaultAllocation == getOptions().allocation_mode;
    }

    /** -----------------------------------------------------------------------*
     * @brief Register a new stream into the device memory manager.
     * 
     * Also returns success if the stream is already registered.
     * 
     * @param stream The stream to register
     * @return rmmError_t RMM_SUCCESS if all goes well,
     *                    RMM_ERROR_INVALID_ARGUMENT if the stream is invalid.
     * ----------------------------------------------------------------------**/
    rmmError_t registerStream(cudaStream_t stream);

private:
    BlazingMemoryManager() = default;
    ~BlazingMemoryManager() = default;
    BlazingMemoryManager(const BlazingMemoryManager&) = delete;
    BlazingMemoryManager& operator=(const BlazingMemoryManager&) = delete;
    std::mutex manager_mutex;
    std::set<cudaStream_t> registered_streams;

    rmmOptions_t options{};
    bool is_initialized{false};

    std::unique_ptr<rmm::mr::device_memory_resource> initialized_resource{};
};

#endif // BLAZING_MEMORY_MANAGER_H
