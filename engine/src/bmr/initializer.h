#pragma once 

#include <bmr/BlazingMemoryResource.h>

// Shutdown memory manager.
static void BlazingRMMFinalize()
{
	blazing_device_memory_resource::getInstance().finalize();	
}

static void BlazingRMMInitialize(std::string allocation_mode = "managed_memory_resource",
										std::size_t initial_pool_size = 0,
										float device_mem_resouce_consumption_thresh = 0.95)
{
  blazing_device_memory_resource::getInstance().initialize(
	  allocation_mode, initial_pool_size, device_mem_resouce_consumption_thresh); 	
}
