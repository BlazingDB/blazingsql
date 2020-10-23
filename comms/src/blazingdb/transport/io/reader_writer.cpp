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
  for (int bufferIndex = 0; bufferIndex < this->numBuffers; bufferIndex++) {
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
  PinnedBuffer *buffer = new PinnedBuffer();
  buffer->size = this->bufferSize;
  allocations.resize(allocations.size() + 1);
  std::size_t num_new_buffers = this->numBuffers/2;
  cudaError_t err = cudaMallocHost((void **)&allocations[allocations.size() -1], this->bufferSize * num_new_buffers);
  for (int bufferIndex = 0; bufferIndex < num_new_buffers; bufferIndex++) {
    PinnedBuffer *buffer = new PinnedBuffer();
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
  cv.notify_one();
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

void writeBuffersFromGPUTCP(std::vector<ColumnTransport> &column_transport,
                            std::vector<std::size_t> bufferSizes,
                            std::vector<const char *> buffers, void *fileDescriptor,
                            int gpuNum) {
  if (bufferSizes.size() == 0) {
    return;
  }
  struct queue_item {
    std::size_t bufferIndex{};
    std::size_t chunkIndex{};
    PinnedBuffer *chunk{nullptr};
    std::size_t chunk_size{};

    bool operator<(const queue_item &item) const {
      if (bufferIndex == item.bufferIndex) {
        return chunkIndex > item.chunkIndex;
      } else {
        return bufferIndex > item.bufferIndex;
      }
    }

    bool operator==(const queue_item &item) const {
      return ((bufferIndex == item.bufferIndex) &&
              (chunkIndex == item.chunkIndex));
    }
  };
  std::priority_queue<queue_item> writePairs;
  std::condition_variable cv;
  std::mutex writeMutex;
  std::vector<char *> tempReadAllocations(bufferSizes.size());
  std::vector<BlazingThread> copyThreads(bufferSizes.size());
  std::size_t amountWrittenTotalTotal = 0;

  std::vector<queue_item> writeOrder;
  for (size_t bufferIndex = 0; bufferIndex < bufferSizes.size();
       bufferIndex++) {
    std::size_t amountWrittenTotal = 0;
    size_t chunkIndex = 0;
    do {
      writeOrder.push_back(
          {.bufferIndex = bufferIndex, .chunkIndex = chunkIndex});
      amountWrittenTotal += getPinnedBufferProvider().sizeBuffers();
      chunkIndex++;
    } while (amountWrittenTotal < bufferSizes[bufferIndex]);
  }

  // buffer is from gpu or is from cpu
  for (size_t bufferIndex = 0; bufferIndex < bufferSizes.size();
       bufferIndex++) {
    copyThreads[bufferIndex] = BlazingThread(
        [bufferIndex, &cv, &amountWrittenTotalTotal, &writeMutex, &buffers,
         &writePairs, &writeOrder, &bufferSizes, &tempReadAllocations,
         fileDescriptor, gpuNum]() {
          cudaSetDevice(gpuNum);
          std::size_t amountWrittenTotal = 0;
          size_t chunkIndex = 0;
          do {
            PinnedBuffer *buffer = getPinnedBufferProvider().getBuffer();
            std::size_t amountToWrite;
            if ((bufferSizes[bufferIndex] - amountWrittenTotal) > buffer->size)
              amountToWrite = buffer->size;
            else
              amountToWrite = bufferSizes[bufferIndex] - amountWrittenTotal;

            cudaSetDevice(gpuNum);
            cudaMemcpyAsync(buffer->data,
                            buffers[bufferIndex] + amountWrittenTotal,
                            amountToWrite, cudaMemcpyDeviceToHost, nullptr);
            cudaStreamSynchronize(nullptr);
            {
              std::unique_lock<std::mutex> lock(writeMutex);
              writePairs.push(queue_item{.bufferIndex = bufferIndex,
                                         .chunkIndex = chunkIndex,
                                         .chunk = buffer,
                                         .chunk_size = amountToWrite});
              chunkIndex++;
              amountWrittenTotal += amountToWrite;
              cv.notify_one();
            }
          } while (amountWrittenTotal < bufferSizes[bufferIndex]);
          {
            std::lock_guard<std::mutex> lock(writeMutex);
            amountWrittenTotalTotal += amountWrittenTotal;
          }
        });
  }

  BlazingThread writeThread =
      BlazingThread([fileDescriptor, &writePairs, &bufferSizes, writeOrder,
                   &writeMutex, &cv] {
        PinnedBuffer *buffer = nullptr;
        std::size_t amountToWrite;
        queue_item item;
        std::size_t writeIndex = 0;
        bool started = false;
        do {
          {
            CodeTimer blazing_timer;
            std::unique_lock<std::mutex> lock(writeMutex);
            while(!cv.wait_for(lock, 60000ms, [&writePairs, &writeOrder, writeIndex, &blazing_timer] {
                bool wrote = !writePairs.empty() &&
                      writeOrder[writeIndex] == writePairs.top();
                
                if (!wrote && blazing_timer.elapsed_time() > 59000){
                  auto logger = spdlog::get("batch_logger");
                  logger->warn("|||{info}|{duration}||||",
                              "info"_a="writeBuffersFromGPUTCP timed out",
                              "duration"_a=blazing_timer.elapsed_time());
                }
                return wrote;
              })){}

            item = writePairs.top();
            amountToWrite = item.chunk_size;
            buffer = item.chunk;
            started = false;
            writePairs.pop();
          }

          if (buffer != nullptr) {
            std::lock_guard<std::mutex> lock(writeMutex);
            std::size_t amountWritten = blazingdb::transport::io::writeToSocket(
                fileDescriptor, (char *)buffer->data, amountToWrite);
            writeIndex++;
            if (amountWritten != amountToWrite) {
              getPinnedBufferProvider().freeBuffer(buffer);
              throw std::exception();
            }
            getPinnedBufferProvider().freeBuffer(buffer);
          }
        } while (writeIndex < writeOrder.size());
      });

  for (std::size_t threadIndex = 0; threadIndex < copyThreads.size();
       threadIndex++) {
    copyThreads[threadIndex].join();
  }
  {
    std::unique_lock<std::mutex> lock(writeMutex);
    writePairs.push({.bufferIndex = INT_MAX,
                     .chunkIndex = INT_MAX,
                     .chunk = nullptr,
                     .chunk_size = amountWrittenTotalTotal});
    cv.notify_one();
  }
  writeThread.join();
  PinnedBuffer *buffer = getPinnedBufferProvider().getBuffer();
  getPinnedBufferProvider().freeBuffer(buffer);
}

void readBuffersIntoGPUTCP(std::vector<std::size_t> bufferSizes,
                                          void *fileDescriptor, int gpuNum, std::vector<rmm::device_buffer> &tempReadAllocations) 
{
  for (int bufferIndex = 0; bufferIndex < bufferSizes.size(); bufferIndex++) {
    cudaSetDevice(gpuNum);
    tempReadAllocations.emplace_back(rmm::device_buffer(bufferSizes[bufferIndex]));
  }
  for (int bufferIndex = 0; bufferIndex < bufferSizes.size(); bufferIndex++) {
    std::vector<BlazingThread> copyThreads;
    std::size_t amountReadTotal = 0;
    do {
      PinnedBuffer *buffer = getPinnedBufferProvider().getBuffer();
      std::size_t amountToRead =
          (bufferSizes[bufferIndex] - amountReadTotal) > buffer->size
              ? buffer->size
              : bufferSizes[bufferIndex] - amountReadTotal;

      std::size_t amountRead =
          blazingdb::transport::io::readFromSocket(fileDescriptor, (char *)buffer->data, amountToRead);

      assert(amountRead == amountToRead);
      if (amountRead != amountToRead) {
        getPinnedBufferProvider().freeBuffer(buffer);
        throw std::exception();
      }
      copyThreads.push_back(BlazingThread(
          [&tempReadAllocations, &bufferSizes, bufferIndex,
           buffer, amountRead, amountReadTotal, gpuNum]() {
            cudaSetDevice(gpuNum);
            cudaMemcpyAsync(tempReadAllocations[bufferIndex].data() + amountReadTotal,
                            buffer->data, amountRead, cudaMemcpyHostToDevice,
                            nullptr);
            getPinnedBufferProvider().freeBuffer(buffer);
            cudaStreamSynchronize(nullptr);
          }));
      amountReadTotal += amountRead;

    } while (amountReadTotal < bufferSizes[bufferIndex]);
    for (std::size_t threadIndex = 0; threadIndex < copyThreads.size(); threadIndex++) {
      copyThreads[threadIndex].join();
    }
  }
  // return tempReadAllocations;
}

void readBuffersIntoCPUTCP(std::vector<std::size_t> bufferSizes,
                                          void *fileDescriptor, int gpuNum, std::vector<Buffer> & tempReadAllocations)
{
  for (int bufferIndex = 0; bufferIndex < bufferSizes.size(); bufferIndex++) {
    cudaSetDevice(gpuNum);
    tempReadAllocations.emplace_back(Buffer(bufferSizes[bufferIndex], '0'));
  }
  for (int bufferIndex = 0; bufferIndex < bufferSizes.size(); bufferIndex++) {
    std::vector<BlazingThread> copyThreads;
    std::size_t amountReadTotal = 0;
    do {
      PinnedBuffer *buffer = getPinnedBufferProvider().getBuffer();
      std::size_t amountToRead =
          (bufferSizes[bufferIndex] - amountReadTotal) > buffer->size
              ? buffer->size
              : bufferSizes[bufferIndex] - amountReadTotal;

      std::size_t amountRead =
          blazingdb::transport::io::readFromSocket(fileDescriptor, (char *)buffer->data, amountToRead);

      assert(amountRead == amountToRead);
      if (amountRead != amountToRead) {
        getPinnedBufferProvider().freeBuffer(buffer);
        throw std::exception();
      }
      copyThreads.push_back(BlazingThread(
          [&tempReadAllocations, &bufferSizes, bufferIndex,
           buffer, amountRead, amountReadTotal, gpuNum]() {
            cudaSetDevice(gpuNum);
            cudaMemcpyAsync((void *)tempReadAllocations[bufferIndex].data() + amountReadTotal,
                            buffer->data, amountRead, cudaMemcpyHostToHost, // use cudaMemcpyHostToHost for lazy loading into gpu memory
                            nullptr);
            getPinnedBufferProvider().freeBuffer(buffer);
            cudaStreamSynchronize(nullptr);
          }));
      amountReadTotal += amountRead;

    } while (amountReadTotal < bufferSizes[bufferIndex]);
    for (std::size_t threadIndex = 0; threadIndex < copyThreads.size(); threadIndex++) {
      copyThreads[threadIndex].join();
    }
  }

}


}  // namespace io
}  // namespace transport
}  // namespace blazingdb
