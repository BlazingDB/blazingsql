#ifndef UTILS_CUH_
#define UTILS_CUH_

#include "gdf_wrapper/gdf_wrapper.cuh"
#include "gdf_wrapper/utilities/cudf_utils.h"

#include <iostream>
#include <vector>
#include <thrust/functional.h>
#include <thrust/execution_policy.h>
#include <thrust/iterator/constant_iterator.h>
#include <thrust/iterator/discard_iterator.h>
#include <thrust/device_ptr.h>
#include <thrust/sequence.h>

#include <nvstrings/NVCategory.h>
#include <nvstrings/NVStrings.h>

#include <rmm/rmm.h>

#ifndef DEVICE_RESET
#define DEVICE_RESET cudaDeviceReset();
#endif

#define CheckCudaErrors( call )                                      \
{                                                                    \
  cudaError_t cudaStatus = call;                                     \
  if (cudaSuccess != cudaStatus)                                     \
  {                                                                  \
    std::cerr << "ERROR: CUDA Runtime call " << #call                \
              << " in line " << __LINE__                             \
              << " of file " << __FILE__                             \
              << " failed with " << cudaGetErrorString(cudaStatus)   \
              << " (" << cudaStatus << ").\n";                       \
    /* Call cudaGetLastError to try to clear error if the cuda context is not corrupted */ \
    cudaGetLastError();                                              \
    throw std::runtime_error("In " + std::string(#call) + " function: CUDA Runtime call error " + cudaGetErrorName(cudaStatus));\
  }                                                                  \
}

#define STRINGIFY_DETAIL(x) #x
#define RAL_STRINGIFY(x) STRINGIFY_DETAIL(x)

#define RAL_EXPECTS(cond, reason)                            \
  (!!(cond))                                                 \
      ? static_cast<void>(0)                                 \
      : throw cudf::logic_error("Ral failure at: " __FILE__ \
                                ":" RAL_STRINGIFY(__LINE__) ": " reason)

static constexpr int ValidSize = 32;
using ValidType = uint32_t;

static size_t  valid_size(size_t column_length)
{
  const size_t n_ints = (column_length / ValidSize) + ((column_length % ValidSize) ? 1 : 0);
  return n_ints * sizeof(ValidType);
}

// TODO: Convert to templated function
void gdf_sequence(int32_t* data, size_t size, int32_t init_val);

// TODO: Convert to templated function
void gdf_sequence(int32_t* data, size_t size, int32_t init_val, int32_t step);

// Returns a NVCategory* that holds a string repeated N times
NVCategory* repeated_string_category(std::string str, size_t repeat_times);

#endif /* UTILS_CUH_ */
