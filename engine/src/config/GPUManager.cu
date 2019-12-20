#include <cuda.h>
#include <cuda_runtime.h>
#include <exception>
#include "GPUManager.cuh"
#include "Utils.cuh"


namespace ral {
namespace config {

size_t gpuMemorySize() {
	// NOTE if CUDA_VISIBLE_DEVICES is 6 and we use 0 here it means we take 6
	int currentDeviceId = 0;
	struct cudaDeviceProp props;
	CheckCudaErrors( cudaSetDevice(currentDeviceId) );
	cudaGetDeviceProperties(&props, currentDeviceId);
	size_t free, total;
	cudaMemGetInfo(&free, &total);

	return free;
}

}	// namespace config
}	// namespace ral
