#include <cuda.h>
#include <cuda_runtime.h>
#include <exception>
#include "GPUManager.cuh"
#include "Utils.cuh"

long gpuMemorySize() {
	int gpuId = 0;
	
	// To get the total size of the current
	struct cudaDeviceProp props;
	CheckCudaErrors( cudaSetDevice(gpuId) );
	cudaGetDeviceProperties(&props, gpuId);
	size_t free, total;
	cudaMemGetInfo(&free, &total);

	return total;
}
