#ifndef GPUMANAGER_H_
#define GPUMANAGER_H_

#include <memory>
#include <string>

namespace ral {
namespace config {

size_t gpuMemorySize();
size_t gpuUsedMemory();

} // namespace config
} // namespace ral

#endif // GPUMANAGER_H_
