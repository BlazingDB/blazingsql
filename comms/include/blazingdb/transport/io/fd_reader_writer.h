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

std::size_t readFromSocket(void* fileDescriptor, char* buf, std::size_t nbyte);
std::size_t writeToSocket(void* fileDescriptor, char* buf, std::size_t nbyte,
                     bool more = true);

}  // namespace io
}  // namespace transport
}  // namespace blazingdb
