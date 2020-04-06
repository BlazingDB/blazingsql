#include "blazingdb/transport/io/reader_writer.h"
#include <cuda.h>
#include <cuda_runtime_api.h>
#include "blazingdb/transport/io/fd_reader_writer.h"
#include "rmm/rmm.h"

#include <condition_variable>
#include <iostream>
#include <mutex>
#include <stack>
#include <thread>
#include <vector>

#include <cassert>
#include <queue>
#include "blazingdb/transport/ColumnTransport.h"
#include <rmm/device_buffer.hpp>

namespace blazingdb {
namespace transport {
namespace experimental {
namespace io {


// numBuffers should be equal to number of threads
PinnedBufferProvider::PinnedBufferProvider(std::size_t sizeBuffers,
                                           std::size_t numBuffers) {
  for (int bufferIndex = 0; bufferIndex < numBuffers; bufferIndex++) {
    PinnedBuffer *buffer = new PinnedBuffer();
    buffer->size = sizeBuffers;
    this->bufferSize = sizeBuffers;
    cudaError_t err = cudaMallocManaged((void **)&buffer->data, sizeBuffers);
    if (err != cudaSuccess) {
      throw std::exception();
    }
    this->buffers.push(buffer);
  }
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
  PinnedBuffer *buffer = new PinnedBuffer();
  buffer->size = this->bufferSize;
  cudaError_t err = cudaMallocManaged((void **)&buffer->data, this->bufferSize);
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
  while (false == this->buffers.empty()) {
    PinnedBuffer *buffer = this->buffers.top();
    cudaFree(buffer->data);
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

void writeBuffersFromGPUTCP(std::vector<ColumnTransport> &column_transport,
                            std::vector<int> bufferSizes,
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
  std::vector<std::thread> writeThreads(bufferSizes.size());
  std::priority_queue<queue_item> writePairs;
  std::condition_variable cv;
  std::mutex writeMutex;
  std::vector<std::thread> allocationThreads(bufferSizes.size());
  std::vector<char *> tempReadAllocations(bufferSizes.size());
  std::vector<std::thread> copyThreads(bufferSizes.size());
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
    copyThreads[bufferIndex] = std::thread(
        [bufferIndex, &cv, &amountWrittenTotalTotal, &writeMutex, &buffers,
         &writePairs, &writeOrder, &bufferSizes, &tempReadAllocations,
         &allocationThreads, fileDescriptor, gpuNum]() {
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

  std::thread writeThread =
      std::thread([fileDescriptor, &writePairs, &bufferSizes, writeOrder,
                   &writeMutex, &cv] {
        PinnedBuffer *buffer = nullptr;
        std::size_t amountToWrite;
        queue_item item;
        std::size_t writeIndex = 0;
        bool started = false;
        do {
          {
            std::unique_lock<std::mutex> lock(writeMutex);
            cv.wait(lock, [&writePairs, &writeOrder, writeIndex] {
              return !writePairs.empty() &&
                     writeOrder[writeIndex] == writePairs.top();
            });
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

  for (std::size_t threadIndex = 0; threadIndex < writeThreads.size();
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
  getPinnedBufferProvider().freeAll();
}

void readBuffersIntoGPUTCP(std::vector<int> bufferSizes,
                                          void *fileDescriptor, int gpuNum, std::vector<rmm::device_buffer> &tempReadAllocations) 
{
  std::vector<std::thread> allocationThreads(bufferSizes.size());
  std::vector<std::thread> readThreads(bufferSizes.size());
  // std::vector<rmm::device_buffer> tempReadAllocations;
  for (int bufferIndex = 0; bufferIndex < bufferSizes.size(); bufferIndex++) {
    cudaSetDevice(gpuNum);
    tempReadAllocations.emplace_back(rmm::device_buffer(bufferSizes[bufferIndex]));
  }
  for (int bufferIndex = 0; bufferIndex < bufferSizes.size(); bufferIndex++) {
    std::vector<std::thread> copyThreads;
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
      copyThreads.push_back(std::thread(
          [&tempReadAllocations, &bufferSizes, &allocationThreads, bufferIndex,
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


}  // namespace io
}  // namespace experimental
}  // namespace transport
}  // namespace blazingdb


