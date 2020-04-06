#pragma once
#include <condition_variable>
#include <mutex>
#include <stack>
#include <vector>
#include "blazingdb/transport/ColumnTransport.h"
#include <rmm/device_buffer.hpp>

namespace blazingdb {
namespace transport {
namespace experimental {
namespace io {

struct PinnedBuffer {
  std::size_t size;
  char *data;
};

class PinnedBufferProvider {
public:
  PinnedBufferProvider(std::size_t sizeBuffers, std::size_t numBuffers);

  PinnedBuffer *getBuffer();

  void freeBuffer(PinnedBuffer *buffer);

  std::size_t sizeBuffers();

  void freeAll();

private:
  void grow();

  std::condition_variable cv;

  std::mutex inUseMutex;

  std::stack<PinnedBuffer *> buffers;

  std::size_t bufferSize;
};
// Memory Pool
PinnedBufferProvider &getPinnedBufferProvider();

void setPinnedBufferProvider(std::size_t sizeBuffers, std::size_t numBuffers);

void writeBuffersFromGPUTCP(std::vector<ColumnTransport> &column_transport,
                            std::vector<int> bufferSizes,
                            std::vector<const char *> buffers, void *fileDescriptor,
                            int gpuNum);

void readBuffersIntoGPUTCP(std::vector<int> bufferSizes,
                                          void *fileDescriptor, int gpuNum, std::vector<rmm::device_buffer> &);

}  // namespace io
}  // namespace experimental
}  // namespace transport
}  // namespace blazingdb
