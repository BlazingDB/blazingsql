#include "cuDF/Allocator.h"
#include "rmm/rmm.h"
#include <rmm/rmm.h>

namespace cuDF {
namespace Allocator {

const std::string BASE_MESSAGE {"ERROR, cuDF::Allocator, "};

void throwException(rmmError_t error, const std::string & during_msg);

void allocate(void** pointer, std::size_t size, cudaStream_t stream) {
    auto error = RMM_ALLOC(pointer, size, stream);

    if (error != RMM_SUCCESS) {
        std::string during_msg = "During allocate of size: " + std::to_string(size);
        throwException(error, during_msg);
    }
}

void reallocate(void **pointer, std::size_t size, cudaStream_t stream) {
    const void *actual = *pointer;
    // TODO: felipe commented in rmm.h          #define RMM_REALLOC(ptr, new_sz, stream)
    /*
    auto error = RMM_REALLOC(pointer, size, stream);

    if (error != RMM_SUCCESS) {
        throwException(error);
    }
    */
}

void deallocate(void* pointer, cudaStream_t stream) {
    auto error = RMM_FREE(pointer, stream);

// commenting out this error handling because deallocate can fail during clean up of other caught errors, where if we throw an error during an error handling, it can make it really crash
    // if (error != RMM_SUCCESS) {
    //     std::string during_msg = "During deallocate";
    //     if (pointer == nullptr)
    //         during_msg = "During deallocate of nullptr";
    //     throwException(error, during_msg);
    // }
}

void throwException(rmmError_t error, const std::string & during_msg) {
    switch(error) {
    case RMM_ERROR_CUDA_ERROR:
        throw CudaError(during_msg);
    case RMM_ERROR_INVALID_ARGUMENT:
        throw InvalidArgument(during_msg);
    case RMM_ERROR_NOT_INITIALIZED:
        throw NotInitialized(during_msg);
    case RMM_ERROR_OUT_OF_MEMORY:
        throw OutOfMemory(during_msg);
    case RMM_ERROR_UNKNOWN:
        throw Unknown(during_msg);
    case RMM_ERROR_IO:
        throw InputOutput(during_msg);
    default:
        throw Unknown(during_msg);
    }
}


CudfAllocatorError::CudfAllocatorError(std::string&& message)
 : message{message}
{ }

const char* CudfAllocatorError::what() const noexcept {
    return message.c_str();
}

CudaError::CudaError(const std::string & during_msg)
 : CudfAllocatorError(BASE_MESSAGE +
           "RMM_ERROR_CUDA_ERROR:" +
           std::to_string(RMM_ERROR_CUDA_ERROR) +
           ", Error in CUDA " + during_msg)
{ }

InvalidArgument::InvalidArgument(const std::string & during_msg)
 : CudfAllocatorError(BASE_MESSAGE +
           "RMM_ERROR_INVALID_ARGUMENT:" +
           std::to_string(RMM_ERROR_INVALID_ARGUMENT) +
           ", Invalid argument was passed " + during_msg)
{ }

NotInitialized::NotInitialized(const std::string & during_msg)
 : CudfAllocatorError(BASE_MESSAGE +
           "RMM_ERROR_NOT_INITIALIZED:" +
           std::to_string(RMM_ERROR_NOT_INITIALIZED) +
           ", RMM API called before rmmInitialize() " + during_msg)
{ }

OutOfMemory::OutOfMemory(const std::string & during_msg)
 : CudfAllocatorError(BASE_MESSAGE +
           "RMM_ERROR_OUT_OF_MEMORY:" +
           std::to_string(RMM_ERROR_OUT_OF_MEMORY) +
           ", Unable to allocate more memory " + during_msg)
{ }

Unknown::Unknown(const std::string & during_msg)
 : CudfAllocatorError(BASE_MESSAGE +
           "RMM_ERROR_UNKNOWN:" +
           std::to_string(RMM_ERROR_UNKNOWN) +
           ", Unknown error " + during_msg)
{ }

InputOutput::InputOutput(const std::string & during_msg)
 : CudfAllocatorError(BASE_MESSAGE +
           "RMM_ERROR_IO:" +
           std::to_string(RMM_ERROR_IO) +
           ", Stats output error " + during_msg)
{ }

} // namespace Allocator
} // namespace cuDF
