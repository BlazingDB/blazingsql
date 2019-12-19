#include <cuda.h>
#include <cuda_runtime.h>
#include <exception>
#include "GPUManager.cuh"
#include "Utils.cuh"


namespace ral {
namespace config {

GPUManager::GPUManager() : currentDeviceId{0} {
	CheckCudaErrors( cudaGetDeviceCount(&totalDevices) );
}

int GPUManager::getDeviceId() {
	return this->currentDeviceId;
}


GPUManager& GPUManager::getInstance() {
	static GPUManager instance;
	return instance;
}

void GPUManager::initialize(int deviceId) {
	if (deviceId < 0 || deviceId >= totalDevices) {
		const std::string dev_id = std::to_string(deviceId);
		const std::string total_devs = std::to_string(totalDevices);
		const std::string error_msg = "In GPUManager::initialize function: Invalid deviceId " + dev_id + " Total: " + total_devs;
		throw std::runtime_error(error_msg);
	}

	currentDeviceId = deviceId;
	setDevice();
}

void GPUManager::setDevice() {
	CheckCudaErrors( cudaSetDevice(currentDeviceId) );
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
