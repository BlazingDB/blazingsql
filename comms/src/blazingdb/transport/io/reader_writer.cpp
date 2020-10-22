#include "blazingdb/transport/io/reader_writer.h"
#include <cuda.h>
#include <cuda_runtime_api.h>
#include "blazingdb/transport/io/fd_reader_writer.h"

#include <condition_variable>
#include <iostream>
#include <mutex>
#include <stack>
#include <thread>
#include <vector>

#include <cassert>
#include <queue>
#include "blazingdb/transport/ColumnTransport.h"
#include "blazingdb/concurrency/BlazingThread.h"
#include <rmm/device_buffer.hpp>

#include "../engine/src/CodeTimer.h"
using namespace std::chrono_literals;

#include <spdlog/spdlog.h>
using namespace fmt::literals;

namespace blazingdb {
namespace transport {
namespace io {

// numBuffers should be equal to number of threads
PinnedBufferProvider::PinnedBufferProvider(std::size_t sizeBuffers,
                                           std::size_t numBuffers) {
  this->buffer_counter = numBuffers;
  for (int bufferIndex = 0; bufferIndex < numBuffers; bufferIndex++) {
    PinnedBuffer *buffer = new PinnedBuffer();
    buffer->size = sizeBuffers;
    this->bufferSize = sizeBuffers;
    cudaError_t err = cudaMallocHost((void **)&buffer->data, sizeBuffers);
    if (err != cudaSuccess) {
      throw std::exception();
    }
    this->buffers.push(buffer);
  }
}

PinnedBufferProvider::~PinnedBufferProvider(){
    getPinnedBufferProvider().freeAll();
}

// TODO: consider adding some kind of priority
// based on when the request was made
PinnedBuffer *PinnedBufferProvider::getBuffer() {
  std::unique_lock<std::mutex> lock(inUseMutex);
  if (this->buffers.empty()) {
    // cv.wait(lock, [this] { return !this->buffers.empty(); });
    // if wait fail use:
    this->grow();
  }
  PinnedBuffer *temp = this->buffers.top();
  this->buffers.pop();
  return temp;
}

void PinnedBufferProvider::grow() {
  this->buffer_counter++;
  PinnedBuffer *buffer = new PinnedBuffer();
  buffer->size = this->bufferSize;
  cudaError_t err = cudaMallocHost((void **)&buffer->data, this->bufferSize);

  auto logger = spdlog::get("batch_logger");
  std::string log_detail = "PinnedBufferProvider::grow() now buffer_counter = ";
  log_detail += std::to_string(this->buffer_counter);
  log_detail += ", bufferSize: " + std::to_string(this->bufferSize);
  logger->debug("|||{info}|||||","info"_a=log_detail);

  if (err != cudaSuccess) {
    throw std::exception();
  }
  this->buffers.push(buffer);
}

void PinnedBufferProvider::freeBuffer(PinnedBuffer *buffer) {
  std::unique_lock<std::mutex> lock(inUseMutex);
  this->buffers.push(buffer);
  cv.notify_one();
}

void PinnedBufferProvider::freeAll() {
  std::unique_lock<std::mutex> lock(inUseMutex);
  this->buffer_counter = 0;
  while (false == this->buffers.empty()) {
    PinnedBuffer *buffer = this->buffers.top();
    cudaFreeHost(buffer->data);
    delete buffer;
    this->buffers.pop();
  }
}

std::size_t PinnedBufferProvider::sizeBuffers() { return this->bufferSize; }

static std::shared_ptr<PinnedBufferProvider> global_instance{};

void setPinnedBufferProvider(std::size_t sizeBuffers, std::size_t numBuffers) {
  global_instance =
      std::make_shared<PinnedBufferProvider>(sizeBuffers, numBuffers);
}

PinnedBufferProvider &getPinnedBufferProvider() { return *global_instance; }



}  // namespace io
}  // namespace transport
}  // namespace blazingdb
