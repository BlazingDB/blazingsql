#pragma once 

#include <rmm/rmm_api.h>
#include <bmr/BlazingMemoryResource.h>

// Shutdown memory manager.
static rmmError_t BlazingRMMFinalize()
{
	blazing_device_memory_resource::getInstance().finalize();
	return RMM_SUCCESS;
}

static rmmError_t BlazingRMMInitialize(rmmOptions_t *options, float device_mem_resouce_consumption_thresh = 0.95)
{
  blazing_device_memory_resource::getInstance().initialize(options, device_mem_resouce_consumption_thresh);
 	return RMM_SUCCESS;
}

// // Query the initialization state of blazing_device_memory_resource.
// static bool BlazingRMMIsInitialized(rmmOptions_t *options)
// {
//   if (nullptr != options) {
//     *options = blazing_device_memory_resource::getOptions();
//   }
//   return blazing_device_memory_resource::getInstance().isInitialized();
// }