#ifndef GPUMANAGER_H_
#define GPUMANAGER_H_

#include <memory>
#include <string>

namespace ral {
namespace config {

class GPUManager
{
public:
	static GPUManager& getInstance();
	
	void initialize(int deviceId);
	void setDevice();
	size_t gpuMemorySize();
	int getDeviceId();

	GPUManager(GPUManager&&) = delete;
	GPUManager(const GPUManager&) = delete;
	GPUManager& operator=(GPUManager&&) = delete;
	GPUManager& operator=(const GPUManager&) = delete;

private:
	GPUManager();

	int currentDeviceId;
	int totalDevices;
};

} // namespace config
} // namespace ral

#endif // GPUMANAGER_H_
