#include "reader_writer.h"
#include <cuda.h>
#include <cuda_runtime_api.h>

#include <condition_variable>
#include <iostream>
#include <mutex>
#include <stack>
#include <thread>
#include <vector>

#include <cassert>
#include <queue>
#include "transport/ColumnTransport.h"
#include "ExceptionHandling/BlazingThread.h"
#include <rmm/device_buffer.hpp>

#include "CodeTimer.h"
using namespace std::chrono_literals;

#include <spdlog/spdlog.h>
using namespace fmt::literals;

namespace blazingdb {
namespace transport {
namespace io {

PinnedBufferProvider::PinnedBufferProvider(std::size_t sizeBuffers,
                                           std::size_t numBuffers) {
  this->numBuffers = numBuffers;
  this->bufferSize = sizeBuffers;
  this->buffer_counter = numBuffers;
  this->allocations = std::vector<char *>(1);
  cudaError_t err = cudaMallocHost((void **)&allocations[0], this->numBuffers * sizeBuffers);
  if (err != cudaSuccess) {
    throw std::exception();
  }
  for (size_t bufferIndex = 0; bufferIndex < this->numBuffers; bufferIndex++) {
    PinnedBuffer *buffer = new PinnedBuffer();
    buffer->size = this->bufferSize;
    buffer->data = allocations[0] + bufferIndex * this->bufferSize;
    this->buffers.push(buffer);
  }
  this->buffer_counter =  this->numBuffers;
  this->allocation_counter = 0;
}

PinnedBufferProvider::~PinnedBufferProvider(){
  getPinnedBufferProvider().freeAll();
}

// TODO: consider adding some kind of priority
// based on when the request was made
PinnedBuffer *PinnedBufferProvider::getBuffer() {
  std::unique_lock<std::mutex> lock(inUseMutex);
  if (this->buffers.empty()) {
    //if (this->buffer_counter >= 100){
    //    cv.wait(lock, [this] { return !this->buffers.empty(); });        
    //}else{
        this->grow();
    //}
    
  }
  this->allocation_counter++;
  PinnedBuffer *temp = this->buffers.top();
  this->buffers.pop();
  return temp;
}


// Will create a new allocation and grow the buffer pool with this->numBuffers/2 new buffers
// Its not threadsafe and the lock needs to be applied before calling it
void PinnedBufferProvider::grow() {
  allocations.resize(allocations.size() + 1);
  std::size_t num_new_buffers = this->numBuffers/2;
  cudaError_t err = cudaMallocHost(reinterpret_cast<void **>(&allocations[allocations.size() -1]), this->bufferSize * num_new_buffers);
  for (size_t bufferIndex = 0; bufferIndex < num_new_buffers; bufferIndex++) {
    auto *buffer = new PinnedBuffer();
    buffer->size = this->bufferSize;
    buffer->data = allocations[allocations.size() -1] + bufferIndex * this->bufferSize;
    this->buffers.push(buffer);
    this->buffer_counter++;
  }

  auto logger = spdlog::get("batch_logger");
  std::string log_detail = "PinnedBufferProvider::grow() now buffer_counter = ";
  log_detail += std::to_string(this->buffer_counter);
  log_detail += ", bufferSize: " + std::to_string(this->bufferSize);
  logger->debug("|||{info}|||||","info"_a=log_detail);

  if (err != cudaSuccess) {
    throw std::exception();
  }

}

void PinnedBufferProvider::freeBuffer(PinnedBuffer *buffer) {
  std::unique_lock<std::mutex> lock(inUseMutex);
  this->buffers.push(buffer);  
  this->allocation_counter--;

}

void PinnedBufferProvider::freeAll() {
  std::unique_lock<std::mutex> lock(inUseMutex);
  this->buffer_counter = 0;
  while (false == this->buffers.empty()) {
    PinnedBuffer *buffer = this->buffers.top();
    delete buffer;
    this->buffers.pop();
  }
  for(auto allocation : allocations){
    cudaFreeHost(allocation);
  }
    this->allocation_counter = 0;
}

std::size_t PinnedBufferProvider::sizeBuffers() { return this->bufferSize; }

static std::shared_ptr<PinnedBufferProvider> global_instance{};

void setPinnedBufferProvider(std::size_t sizeBuffers, std::size_t numBuffers) {
  global_instance =
      std::make_shared<PinnedBufferProvider>(sizeBuffers, numBuffers);
}


std::size_t PinnedBufferProvider::get_allocated_buffers(){
  return allocation_counter;
}

std::size_t PinnedBufferProvider::get_total_buffers(){
  return buffer_counter;
}

PinnedBufferProvider &getPinnedBufferProvider() { return *global_instance; }



}  // namespace io
}  // namespace transport
}  // namespace blazingdb
