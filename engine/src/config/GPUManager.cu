#include <cuda.h>
#include <cuda_runtime.h>
#include <cudf/utilities/error.hpp>
#include <exception>
#include "GPUManager.cuh"
#include "Utils.cuh"

namespace ral {
namespace config {

size_t gpuMemorySize() {
	int currentDeviceId = 0;
	struct cudaDeviceProp props;
	CUDA_TRY( cudaSetDevice(currentDeviceId) );
	cudaGetDeviceProperties(&props, currentDeviceId);
	size_t free, total;
	cudaMemGetInfo(&free, &total);

	return free;
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
