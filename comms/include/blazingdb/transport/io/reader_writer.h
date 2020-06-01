#pragma once
#include <condition_variable>
#include <mutex>
#include <vector>
#include "blazingdb/transport/ColumnTransport.h"
#include <rmm/device_buffer.hpp>

namespace blazingdb {
namespace transport {
namespace io {

using Buffer = std::basic_string<char>;

struct PinnedBuffer {
  struct deleter {
    void operator()(PinnedBuffer*);
  };

  std::size_t size;
  char *data;
};

class PinnedBufferProvider {
public:
using pinned_buff_ptr = std::unique_ptr<PinnedBuffer, PinnedBuffer::deleter>;
public:
  PinnedBufferProvider(std::size_t sizeBuffers, std::size_t numBuffers);

  pinned_buff_ptr getBuffer();

  void freeBuffer(pinned_buff_ptr buffer);

  std::size_t sizeBuffers();

  void freeAll();

private:
  void grow();

  std::mutex inUseMutex;

  std::vector<pinned_buff_ptr> buffers;

  std::size_t bufferSize;
};
// Memory Pool
PinnedBufferProvider &getPinnedBufferProvider();

void setPinnedBufferProvider(std::size_t sizeBuffers, std::size_t numBuffers);

void writeBuffersFromGPUTCP(std::vector<ColumnTransport> &column_transport,
                            std::vector<std::size_t> bufferSizes,
                            std::vector<const char *> buffers, void *fileDescriptor);

void readBuffersIntoGPUTCP(std::vector<std::size_t> bufferSizes,
                                          void *fileDescriptor, std::vector<rmm::device_buffer> &);

void readBuffersIntoCPUTCP(std::vector<std::size_t> bufferSizes,
                                          void *fileDescriptor, std::vector<Buffer> &);

}  // namespace io
}  // namespace transport
}  // namespace blazingdb
