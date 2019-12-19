#include <cuda.h>
#include <cuda_runtime.h>
#include <exception>
#include "GPUManager.cuh"
#include "Utils.cuh"


namespace ral {
namespace config {

GPUManager::GPUManager() : currentDeviceId{0} {
}

int GPUManager::getDeviceId() {
	return this->currentDeviceId;
}


GPUManager& GPUManager::getInstance() {
	static GPUManager instance;
	return instance;
}

void GPUManager::initialize(int deviceId) {
	currentDeviceId = deviceId;
	setDevice();
}

void GPUManager::setDevice() {
	//CheckCudaErrors( cudaSetDevice(currentDeviceId) );
}

size_t GPUManager::gpuMemorySize() {
	struct cudaDeviceProp props;
	CheckCudaErrors( cudaSetDevice(currentDeviceId) );
	cudaGetDeviceProperties(&props, currentDeviceId);
	size_t free, total;
	cudaMemGetInfo(&free, &total);

	return free;
}

}	// namespace config
}	// namespace ral
