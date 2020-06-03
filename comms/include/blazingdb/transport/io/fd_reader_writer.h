#pragma once
#include <condition_variable>
#include <mutex>
#include <stack>
#include <vector>
#include "blazingdb/transport/ColumnTransport.h"

namespace blazingdb {
namespace transport {
namespace io {

constexpr size_t NUMBER_RETRIES = 20;
constexpr size_t FILE_RETRY_DELAY = 20;

void readFromSocket(void* fileDescriptor, char* buf, size_t nbyte);
void writeToSocket(void* fileDescriptor, const char* buf, size_t nbyte,
                     bool more = true);

}  // namespace io
}  // namespace transport
}  // namespace blazingdb
