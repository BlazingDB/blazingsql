#include "BlazingMemoryResource.h"

// BEGIN internal_blazing_device_memory_resource

// TODO: use another constructor for memory in bytes
internal_blazing_device_memory_resource::internal_blazing_device_memory_resource(std::string allocation_mode,
                                        std::size_t initial_pool_size,
                                        std::size_t maximum_pool_size,
                                        std::string allocator_logging_file,
                                        float custom_threshold)
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
        if (initial_pool_size == 0){
            memory_resource_owner = rmm::mr::make_owning_wrapper<rmm::mr::pool_memory_resource>(
                std::make_shared<rmm::mr::cuda_memory_resource>());
        } else if (maximum_pool_size == 0) {
            memory_resource_owner = rmm::mr::make_owning_wrapper<rmm::mr::pool_memory_resource>(
                std::make_shared<rmm::mr::cuda_memory_resource>(), initial_pool_size);
        } else {
            memory_resource_owner = rmm::mr::make_owning_wrapper<rmm::mr::pool_memory_resource>(
                std::make_shared<rmm::mr::cuda_memory_resource>(), initial_pool_size, maximum_pool_size);
        }
        memory_resource = memory_resource_owner.get();
    } else if (allocation_mode == "managed_pool_memory_resource") {
        if (initial_pool_size == 0){
            memory_resource_owner = rmm::mr::make_owning_wrapper<rmm::mr::pool_memory_resource>(
                std::make_shared<rmm::mr::managed_memory_resource>());
        } else if (maximum_pool_size == 0) {
            memory_resource_owner = rmm::mr::make_owning_wrapper<rmm::mr::pool_memory_resource>(
                std::make_shared<rmm::mr::managed_memory_resource>(), initial_pool_size);
        } else {
            memory_resource_owner = rmm::mr::make_owning_wrapper<rmm::mr::pool_memory_resource>(
                std::make_shared<rmm::mr::managed_memory_resource>(), initial_pool_size, maximum_pool_size);
        }            
        memory_resource = memory_resource_owner.get();
    } else if (allocation_mode == "arena_memory_resource") {
        if (initial_pool_size == 0){
            memory_resource_owner = rmm::mr::make_owning_wrapper<rmm::mr::arena_memory_resource>(
                std::make_shared<rmm::mr::cuda_memory_resource>());
        } else if (maximum_pool_size == 0) {
            memory_resource_owner = rmm::mr::make_owning_wrapper<rmm::mr::arena_memory_resource>(
                std::make_shared<rmm::mr::cuda_memory_resource>(), initial_pool_size);
        } else {
            memory_resource_owner = rmm::mr::make_owning_wrapper<rmm::mr::arena_memory_resource>(
                std::make_shared<rmm::mr::cuda_memory_resource>(), initial_pool_size, maximum_pool_size);
        }         
        
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
    max_used_memory=0;
}

size_t internal_blazing_device_memory_resource::get_memory_used() {
    return used_memory;
}

size_t internal_blazing_device_memory_resource::get_max_memory_used() {
    return max_used_memory;
}

size_t internal_blazing_device_memory_resource::get_from_driver_used_memory() {
    return ral::config::gpuUsedMemory();
}

size_t internal_blazing_device_memory_resource::get_total_memory() {
    return total_memory_size;
}

size_t internal_blazing_device_memory_resource::get_memory_limit() {
    return memory_limit;
}

std::string internal_blazing_device_memory_resource::get_type() {
    return type;
}

bool internal_blazing_device_memory_resource::supports_streams() const noexcept { return memory_resource->supports_streams(); }
bool internal_blazing_device_memory_resource::supports_get_mem_info() const noexcept { return memory_resource->supports_get_mem_info(); }

std::string internal_blazing_device_memory_resource::get_full_memory_summary() {
    std::string summary = "";
    summary += "Memory Resource Summary:: Type: " + this->type;
    summary += " | Used Memory: " + std::to_string(this->used_memory);
    summary += " | Max Used Memory: " + std::to_string(this->max_used_memory);
    summary += " | Available Memory from driver: " + std::to_string(this->get_from_driver_used_memory());
    summary += " | Total Memory: " + std::to_string(this->total_memory_size);
    summary += " | Memory Limit: " + std::to_string(this->memory_limit);
    return summary;
}

void* internal_blazing_device_memory_resource::do_allocate(size_t bytes, rmm::cuda_stream_view stream) {
    if (bytes <= 0) { 
        return nullptr;
    }
    used_memory += bytes;
    if (max_used_memory < used_memory){
        max_used_memory += bytes;
    }

    return memory_resource->allocate(bytes, stream);
}

void internal_blazing_device_memory_resource::do_deallocate(void* p, size_t bytes, rmm::cuda_stream_view stream) {
    if (nullptr == p || bytes == 0) return;
    if (used_memory < bytes) {
        std::cerr << "blazing_device_memory_resource: Deallocating more bytes than used right now, used_memory: " << used_memory.load() << " less than " << bytes << " bytes." << std::endl;
        used_memory = 0;
    } else {
        used_memory -= bytes;
    }

    return memory_resource->deallocate(p, bytes, stream);
}

bool internal_blazing_device_memory_resource::do_is_equal(device_memory_resource const& other) const noexcept {
    return memory_resource->is_equal(other);
}

std::pair<size_t, size_t> internal_blazing_device_memory_resource::do_get_mem_info(rmm::cuda_stream_view stream) const {
    return memory_resource->get_mem_info(stream);
}

// END internal_blazing_device_memory_resource

// BEGIN blazing_device_memory_resource

size_t blazing_device_memory_resource::get_memory_used() {
    return initialized_resource->get_memory_used();
}

size_t blazing_device_memory_resource::get_max_memory_used() {
    return initialized_resource->get_max_memory_used();
}

size_t blazing_device_memory_resource::get_total_memory() {
    return initialized_resource->get_total_memory() ;
}

size_t blazing_device_memory_resource::get_from_driver_used_memory()  {
    return initialized_resource->get_from_driver_used_memory();
}
size_t blazing_device_memory_resource::get_memory_limit() {
    return initialized_resource->get_memory_limit() ;
}

std::string blazing_device_memory_resource::get_type() {
    return initialized_resource->get_type() ;
}

std::string blazing_device_memory_resource::get_full_memory_summary() {
    return initialized_resource->get_full_memory_summary() ;
}

void blazing_device_memory_resource::initialize(std::string allocation_mode,
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

void blazing_device_memory_resource::finalize(){
    std::lock_guard<std::mutex> guard(manager_mutex);

    // finalization before initialization is a no-op
    if (isInitialized()) {
        registered_streams.clear();
        initialized_resource.reset();
        is_initialized = false;
    }
}

bool blazing_device_memory_resource::isInitialized() {
    return getInstance().is_initialized;
}

// END blazing_device_memory_resource

// BEGIN internal_blazing_host_memory_resource

// TODO: percy,cordova. Improve the design of get memory in real time 
internal_blazing_host_memory_resource::internal_blazing_host_memory_resource(float custom_threshold)
{
    struct sysinfo si;
    if (sysinfo(&si) < 0) {
        std::cerr << "@@ error sysinfo host "<< std::endl;
    } 
    total_memory_size = (size_t)si.freeram;
    used_memory_size = 0;
    memory_limit = custom_threshold * total_memory_size;
}

void internal_blazing_host_memory_resource::allocate(std::size_t bytes)  {
    used_memory_size +=  bytes;
}

void internal_blazing_host_memory_resource::deallocate(std::size_t bytes)  {
    used_memory_size -= bytes;
}

size_t internal_blazing_host_memory_resource::get_from_driver_used_memory()  {
    struct sysinfo si;
    sysinfo (&si);
    // NOTE: sync point 
    total_memory_size = (size_t)si.totalram;
    used_memory_size = total_memory_size - (size_t)si.freeram;;
    return used_memory_size;
}

size_t internal_blazing_host_memory_resource::get_memory_used() {
    return used_memory_size;
}

size_t internal_blazing_host_memory_resource::get_total_memory() {
    return total_memory_size;
}

size_t internal_blazing_host_memory_resource::get_memory_limit() {
    return memory_limit;
}

// END internal_blazing_host_memory_resource

// BEGIN blazing_host_memory_resource

    size_t blazing_host_memory_resource::get_memory_used() {
        return initialized_resource->get_memory_used();
    }

    size_t blazing_host_memory_resource::get_total_memory() {
        return initialized_resource->get_total_memory() ;
    }

    size_t blazing_host_memory_resource::get_from_driver_used_memory() {
        return initialized_resource->get_from_driver_used_memory();
    }

    size_t blazing_host_memory_resource::get_memory_limit() {
        return initialized_resource->get_memory_limit() ;
    }

    void blazing_host_memory_resource::allocate(std::size_t bytes) {
        initialized_resource->allocate(bytes);
    }

    void blazing_host_memory_resource::deallocate(std::size_t bytes) {
        initialized_resource->deallocate(bytes);
    }

    void blazing_host_memory_resource::initialize(float host_mem_resouce_consumption_thresh) {
        
        std::lock_guard<std::mutex> guard(manager_mutex);

        // repeat initialization is a no-op
        if (isInitialized()) return;

        initialized_resource.reset(new internal_blazing_host_memory_resource(host_mem_resouce_consumption_thresh));

        is_initialized = true;
    }

    void blazing_host_memory_resource::finalize() {
        std::lock_guard<std::mutex> guard(manager_mutex);

        // finalization before initialization is a no-op
        if (isInitialized()) {
            initialized_resource.reset();
            is_initialized = false;
        }
    }

    bool blazing_host_memory_resource::isInitialized() {
        return getInstance().is_initialized;
    }

// END blazing_host_memory_resource

// BEGIN blazing_disk_memory_resource

// TODO: percy, cordova.Improve the design of get memory in real time 
blazing_disk_memory_resource::blazing_disk_memory_resource(float custom_threshold) {
    struct statvfs stat_disk;
    int ret = statvfs("/", &stat_disk);

    total_memory_size = (size_t)(stat_disk.f_blocks * stat_disk.f_frsize);
    size_t available_disk_size = (size_t)(stat_disk.f_bfree * stat_disk.f_frsize);
    used_memory_size = total_memory_size - available_disk_size;

    memory_limit = custom_threshold *  total_memory_size;
}

size_t blazing_disk_memory_resource::get_from_driver_used_memory() {
    struct sysinfo si;
    sysinfo (&si);
    // NOTE: sync point 
    total_memory_size = (size_t)si.totalram;
    used_memory_size =  total_memory_size - (size_t)si.freeram;
    return used_memory_size;
}

size_t blazing_disk_memory_resource::get_memory_limit() {
    return memory_limit;
}

size_t blazing_disk_memory_resource::get_memory_used() {
    return used_memory_size;
}

size_t blazing_disk_memory_resource::get_total_memory() {
    return total_memory_size;
}

// END blazing_disk_memory_resource