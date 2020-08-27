#include <cuda.h>
#include <cuda_runtime.h>
#include <cudf/utilities/error.hpp>
#include "GPUManager.cuh"

namespace ral {
namespace config {

size_t gpuFreeMemory() {
	int currentDeviceId = 0;
	struct cudaDeviceProp props;
	CUDA_TRY( cudaSetDevice(currentDeviceId) );
	cudaGetDeviceProperties(&props, currentDeviceId);
	size_t free, total;
	cudaMemGetInfo(&free, &total);

	return free;
}

size_t gpuTotalMemory() {
	int nDevices;

	cudaGetDeviceCount(&nDevices);

	size_t total_memory;
	size_t free, total;
	struct cudaDeviceProp props;
	for (int gpu_id = 0; gpu_id < nDevices; ++gpu_id) {
		CUDA_TRY( cudaSetDevice(gpu_id) );
		cudaGetDeviceProperties(&props, gpu_id);
		cudaMemGetInfo(&free, &total);
		total_memory += total;
	}
	return total_memory;
}

size_t gpuUsedMemory() {
	int currentDeviceId = 0;
	struct cudaDeviceProp props;
	CUDA_TRY( cudaSetDevice(currentDeviceId) );
	cudaGetDeviceProperties(&props, currentDeviceId);
	size_t free, total;
	cudaMemGetInfo(&free, &total);

	return total - free;
}

}	// namespace config
}	// namespace ral
