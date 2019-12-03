#pragma once

#include <cuda_runtime_api.h>
#include <exception>
#include <string>

namespace cuDF {
namespace Allocator {

void allocate(void ** pointer, std::size_t size, cudaStream_t stream = 0);

void reallocate(void ** pointer, std::size_t size, cudaStream_t stream = 0);

void deallocate(void * pointer, cudaStream_t stream = 0);


class CudfAllocatorError : public std::exception {
public:
	CudfAllocatorError(std::string && message);
	const char * what() const noexcept override;

private:
	const std::string message;
};

class CudaError : public CudfAllocatorError {
public:
	CudaError(const std::string & during_msg);
};

class InvalidArgument : public CudfAllocatorError {
public:
	InvalidArgument(const std::string & during_msg);
};

class NotInitialized : public CudfAllocatorError {
public:
	NotInitialized(const std::string & during_msg);
};

class OutOfMemory : public CudfAllocatorError {
public:
	OutOfMemory(const std::string & during_msg);
};

class InputOutput : public CudfAllocatorError {
public:
	InputOutput(const std::string & during_msg);
};

class Unknown : public CudfAllocatorError {
public:
	Unknown(const std::string & during_msg);
};

}  // namespace Allocator
}  // namespace cuDF
