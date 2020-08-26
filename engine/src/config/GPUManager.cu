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

size_t gpuFreeMemoryPerDevice(int currentDeviceId) {
	int nDevices;

	cudaGetDeviceCount(&nDevices);
	
	// if we have multiple workers with a single GPU
	if (nDevices == 1) {
		currentDeviceId = 0;
	}

	struct cudaDeviceProp props;
	CUDA_TRY( cudaSetDevice(currentDeviceId) );
	cudaGetDeviceProperties(&props, currentDeviceId);
	size_t free, total;
	cudaMemGetInfo(&free, &total);

	return free;
}

size_t gpuTotalMemory() {
	int currentDeviceId = 0;
	struct cudaDeviceProp props;
	CUDA_TRY( cudaSetDevice(currentDeviceId) );
	cudaGetDeviceProperties(&props, currentDeviceId);
	size_t free, total;
	cudaMemGetInfo(&free, &total);

	return total;
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
