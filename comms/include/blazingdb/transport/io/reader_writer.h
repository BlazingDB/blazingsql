#pragma once
#include <condition_variable>
#include <mutex>
#include <stack>
#include <vector>
#include "blazingdb/transport/ColumnTransport.h"
#include <rmm/device_buffer.hpp>

namespace blazingdb {
namespace transport {
namespace io {


using Buffer = std::basic_string<char>;

struct PinnedBuffer {
  std::size_t size;
  char *data;
  std::size_t use_size;
};

class PinnedBufferProvider {
public:
  PinnedBufferProvider(std::size_t sizeBuffers, std::size_t numBuffers);

  ~PinnedBufferProvider();

  PinnedBuffer *getBuffer();

  void freeBuffer(PinnedBuffer *buffer);

  std::size_t sizeBuffers();

  void freeAll();

private:
  // Its not threadsafe and the lock needs to be applied before calling it
  void grow();

  std::condition_variable cv;

  std::mutex inUseMutex;

  std::stack<PinnedBuffer *> buffers;

  std::size_t bufferSize;

  std::size_t numBuffers;

  int buffer_counter;

  int allocation_counter;
    
  std::vector<char *> allocations;
};

// Memory Pool
PinnedBufferProvider &getPinnedBufferProvider();

void setPinnedBufferProvider(std::size_t sizeBuffers, std::size_t numBuffers);


}  // namespace io
}  // namespace transport
}  // namespace blazingdb
