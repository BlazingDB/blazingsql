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

#define CUDF_CALL( call )                                            \
{                                                                    \
  gdf_error err = call;                                              \
  if ( err != GDF_SUCCESS )                                          \
  {                                                                  \
    std::cerr << "ERROR: CUDF Runtime call " << #call                \
              << " in line " << __LINE__                             \
              << " of file " << __FILE__                             \
              << " failed with " << gdf_error_get_name(err)          \
              << " (" << err << ").\n";                              \
    /* Call cudaGetLastError to try to clear error if the cuda context is not corrupted */ \
    cudaGetLastError();                                              \
    throw std::runtime_error("In " + std::string(#call) + " function: CUDF Runtime call error " + gdf_error_get_name(err));\
  }                                                               \
}

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

// Type for a unique_ptr to a gdf_column with a custom deleter
// Custom deleter is defined at construction
using gdf_col_pointer = typename std::unique_ptr<gdf_column,
                                                 std::function<void(gdf_column*)>>;

template <typename col_type>
void print_typed_column(col_type * col_data,
                        cudf::valid_type * validity_mask,
                        const size_t num_rows)
{

  std::vector<col_type> h_data(num_rows);
  CheckCudaErrors(cudaMemcpy(h_data.data(), col_data, num_rows * sizeof(col_type), cudaMemcpyDeviceToHost));


  const size_t num_masks = valid_size(num_rows);
  std::vector<cudf::valid_type> h_mask(num_masks);
  if(nullptr != validity_mask)
  {
    CheckCudaErrors(cudaMemcpy((int *) h_mask.data(), validity_mask, num_masks * sizeof(cudf::valid_type), cudaMemcpyDeviceToHost));
  }

  if (validity_mask == nullptr) {
    for(size_t i = 0; i < num_rows; ++i)
    {
      if (sizeof(col_type) == 1)
        std::cout << (int)h_data[i] << " ";
      else
        std::cout << h_data[i] << " ";
    }
  }
  else {
    for(size_t i = 0; i < num_rows; ++i)
    {
        std::cout << "(" << std::to_string(h_data[i]) << "|" << gdf_is_valid(h_mask.data(), i) << "), ";
    }
  }
  std::cout << std::endl;
}

static void print_gdf_column(gdf_column const * the_column)
{
  const size_t num_rows = the_column->size;

  const cudf::type_id gdf_col_type = to_type_id(the_column->dtype);
  switch(gdf_col_type)
  {
    case GDF_BOOL8:
      {
        using col_type = gdf_bool8;
        col_type * col_data = static_cast<col_type*>(the_column->data);
        print_typed_column<col_type>(col_data, the_column->valid, num_rows);
        break;
      }
    case GDF_INT8:
      {
        using col_type = int8_t;
        col_type * col_data = static_cast<col_type*>(the_column->data);
        print_typed_column<col_type>(col_data, the_column->valid, num_rows);
        break;
      }
    case GDF_INT16:
      {
        using col_type = int16_t;
        col_type * col_data = static_cast<col_type*>(the_column->data);
        print_typed_column<col_type>(col_data, the_column->valid, num_rows);
        break;
      }
    case GDF_INT32:
      {
        using col_type = int32_t;
        col_type * col_data = static_cast<col_type*>(the_column->data);
        print_typed_column<col_type>(col_data, the_column->valid, num_rows);
        break;
      }
    case GDF_INT64:
      {
        using col_type = int64_t;
        col_type * col_data = static_cast<col_type*>(the_column->data);
        print_typed_column<col_type>(col_data, the_column->valid, num_rows);
        break;
      }
    case GDF_FLOAT32:
      {
        using col_type = float;
        col_type * col_data = static_cast<col_type*>(the_column->data);
        print_typed_column<col_type>(col_data, the_column->valid, num_rows);
        break;
      }
    case GDF_FLOAT64:
      {
        using col_type = double;
        col_type * col_data = static_cast<col_type*>(the_column->data);
        print_typed_column<col_type>(col_data, the_column->valid, num_rows);
        break;
      }
    case GDF_STRING_CATEGORY:
      {
        std::cout<<"Data on column:\n";
        using col_type = int32_t;
        col_type * col_data = static_cast<col_type*>(the_column->data);
        print_typed_column<col_type>(col_data, the_column->valid, num_rows);

        if(the_column->dtype_info.category != nullptr){
          std::cout<<"Data on category:\n";
          size_t keys_size = static_cast<NVCategory *>(the_column->dtype_info.category)->keys_size();
          if(keys_size>0){
            static_cast<NVCategory *>(the_column->dtype_info.category)->get_keys()->print();
          }
          else{
            std::cout<<"Empty!\n";
          }
        }
        else{
            std::cout<<"Category nulled!\n";
        }
        break;
      }
    default:
      {
        std::cout << "Attempted to print unsupported type.\n";
      }
  }
}

void free_gdf_column(gdf_column * column);

// TODO: Convert to templated function
void gdf_sequence(int32_t* data, size_t size, int32_t init_val);

// TODO: Convert to templated function
void gdf_sequence(int32_t* data, size_t size, int32_t init_val, int32_t step);

// Returns a NVCategory* that holds a string repeated N times
NVCategory* repeated_string_category(std::string str, size_t repeat_times);

#endif /* UTILS_CUH_ */
