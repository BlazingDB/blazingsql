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

}	// namespace config
}	// namespace ral
