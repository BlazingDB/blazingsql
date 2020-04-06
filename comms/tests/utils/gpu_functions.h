//
// Created by aocsa on 9/21/19.
//

#ifndef BLAZINGDB_GPU_FUNCTIONS_H
#define BLAZINGDB_GPU_FUNCTIONS_H
#include <cstring>
#include <cuda_runtime_api.h>
#include <cudf.h>
#include <driver_types.h>
#include "Traits/RuntimeTraits.h"
#include "column_factory.h"
#include "StringInfo.h"

namespace blazingdb {
namespace test {

#define CheckCudaErrors(call)                                               \
  {                                                                         \
    cudaError_t cudaStatus = call;                                          \
    if (cudaSuccess != cudaStatus) {                                        \
      std::cerr << "ERROR: CUDA Runtime call " << #call << " in line "      \
                << __LINE__ << " of file " << __FILE__ << " failed with "   \
                << cudaGetErrorString(cudaStatus) << " (" << cudaStatus     \
                << ").\n";                                                  \
      /* Call cudaGetLastError to try to clear error if the cuda context is \
       * not corrupted */                                                   \
      cudaGetLastError();                                                   \
      throw std::runtime_error("In " + std::string(#call) +                 \
                               " function: CUDA Runtime call error " +      \
                               cudaGetErrorName(cudaStatus));               \
    }                                                                       \
  }

class StringsInfo;
class StringInfo;

struct GpuFunctions {
  using DType = gdf_dtype;
  using DTypeInfo = gdf_dtype_extra_info;

  using DataTypePointer = void *;
  using ValidTypePointer = gdf_valid_type *;

  static std::size_t getDataCapacity(gdf_column *column) {
    return blazingdb::test::get_data_size_in_bytes(column);
  }

  static std::size_t getValidCapacity(gdf_column *column) {
    return column->null_count > 0
               ? blazingdb::test::get_bitmask_size_in_bytes(column->size)
               : 0;
  }

  static std::size_t getDTypeSize(gdf_dtype type) {
    return blazingdb::test::get_dtype_size_in_bytes(type);
  }

  static std::size_t isGdfString(gdf_column *column) {
    return (GDF_STRING == column->dtype) ||
           (GDF_STRING_CATEGORY == column->dtype);
  }
  // static const StringsInfo *createStringsInfo(
  //     std::vector<gdf_column *> &columns) {
  //   const StringsInfo *stringsInfo = new StringsInfo{columns};
  //   return stringsInfo;
  // }
  // static std::size_t getStringsCapacity(const StringsInfo *stringsInfo) {
  //   return stringsInfo->capacity();
  // }

  // static void destroyStringsInfo(const StringsInfo *stringsInfo) {
  //   delete stringsInfo;
  // }

//   static void copyGpuToCpu(std::size_t &binary_pointer, std::string &result,
//                            gdf_column *column, const StringsInfo *stringsInfo) {
//     if (column->size == 0) {
//       return;
//     }
//     if (isGdfString(column)) {
//       const StringInfo &stringInfo = stringsInfo->At(column);
//       const std::size_t stringsSize = stringInfo.stringsSize();
//       const std::size_t offsetsSize = stringInfo.offsetsSize();
//       const std::size_t nullMaskSize = stringInfo.nullMaskSize();
//       const std::size_t stringsLength = stringInfo.stringsLength();

//       std::memcpy(&result[binary_pointer], &stringsSize,
//                   sizeof(const std::size_t));
//       std::memcpy(&result[binary_pointer + sizeof(const std::size_t)],
//                   &offsetsSize, sizeof(const std::size_t));
//       std::memcpy(&result[binary_pointer + 2 * sizeof(const std::size_t)],
//                   &nullMaskSize, sizeof(const std::size_t));
//       std::memcpy(&result[binary_pointer + 3 * sizeof(const std::size_t)],
//                   &stringsLength, sizeof(const std::size_t));
//       std::memcpy(&result[binary_pointer + 4 * sizeof(const std::size_t)],
//                   stringInfo.stringsPointer(), stringsSize);
//       std::memcpy(
//           &result[binary_pointer + 4 * sizeof(const std::size_t) + stringsSize],
//           stringInfo.offsetsPointer(), offsetsSize);
//       std::memcpy(&result[binary_pointer + 4 * sizeof(const std::size_t) +
//                           stringsSize + offsetsSize],
//                   stringInfo.nullBitmask(), nullMaskSize);

//       binary_pointer += stringInfo.totalSize();
//     } else {
//       std::size_t data_size = getDataCapacity(column);
//       CheckCudaErrors(cudaMemcpy(&result[binary_pointer], column->data,
//                                  data_size, cudaMemcpyDeviceToHost));
//       binary_pointer += data_size;

//       std::size_t valid_size = getValidCapacity(column);
//       CheckCudaErrors(cudaMemcpy(&result[binary_pointer], column->valid,
//                                  valid_size, cudaMemcpyDeviceToHost));
//       binary_pointer += valid_size;
//     }
//   }
};

inline bool gdf_is_valid(const gdf_valid_type *valid, gdf_size_type pos) {
  if (valid)
    return (valid[pos / GDF_VALID_BITSIZE] >> (pos % GDF_VALID_BITSIZE)) & 1;
  else
    return true;
}

template <typename col_type>
void print_typed_column(col_type *col_data, gdf_valid_type *validity_mask,
                        const size_t num_rows) {
  std::vector<col_type> h_data(num_rows);
  CheckCudaErrors(cudaMemcpy(h_data.data(), col_data,
                             num_rows * sizeof(col_type),
                             cudaMemcpyDeviceToHost));

  const size_t num_masks = gdf_num_bitmask_elements(num_rows);
  std::vector<gdf_valid_type> h_mask(num_masks);
  if (nullptr != validity_mask) {
    CheckCudaErrors(cudaMemcpy((int *)h_mask.data(), validity_mask,
                               num_masks * sizeof(gdf_valid_type),
                               cudaMemcpyDeviceToHost));
  }

  if (validity_mask == nullptr) {
    for (size_t i = 0; i < num_rows; ++i) {
      if (sizeof(col_type) == 1)
        std::cout << (int)h_data[i] << " ";
      else
        std::cout << h_data[i] << " ";
    }
  } else {
    for (size_t i = 0; i < num_rows; ++i) {
      if (gdf_is_valid(h_mask.data(), i))
        std::cout << std::to_string(h_data[i]) << " ";
      else
        std::cout << "@ ";
    }
  }
  std::cout << std::endl;
}

static void print_gdf_column(gdf_column const *the_column) {
  const size_t num_rows = the_column->size;

  const gdf_dtype gdf_col_type = the_column->dtype;
  switch (gdf_col_type) {
    case GDF_INT8: {
      using col_type = int8_t;
      col_type *col_data = static_cast<col_type *>(the_column->data);
      print_typed_column<col_type>(col_data, the_column->valid, num_rows);
      break;
    }
    case GDF_INT16: {
      using col_type = int16_t;
      col_type *col_data = static_cast<col_type *>(the_column->data);
      print_typed_column<col_type>(col_data, the_column->valid, num_rows);
      break;
    }
    case GDF_INT32: {
      using col_type = int32_t;
      col_type *col_data = static_cast<col_type *>(the_column->data);
      print_typed_column<col_type>(col_data, the_column->valid, num_rows);
      break;
    }
    case GDF_INT64: {
      using col_type = int64_t;
      col_type *col_data = static_cast<col_type *>(the_column->data);
      print_typed_column<col_type>(col_data, the_column->valid, num_rows);
      break;
    }
    case GDF_FLOAT32: {
      using col_type = float;
      col_type *col_data = static_cast<col_type *>(the_column->data);
      print_typed_column<col_type>(col_data, the_column->valid, num_rows);
      break;
    }
    case GDF_FLOAT64: {
      using col_type = double;
      col_type *col_data = static_cast<col_type *>(the_column->data);
      print_typed_column<col_type>(col_data, the_column->valid, num_rows);
      break;
    }
    case GDF_STRING_CATEGORY: {
      std::cout << "GDF_STRING_CATEGORY: Data on column:\n";
      using col_type = int32_t;
      col_type *col_data = static_cast<col_type *>(the_column->data);
      print_typed_column<col_type>(col_data, the_column->valid, num_rows);

      if (the_column->dtype_info.category != nullptr) {
        std::cout << "GDF_STRING_CATEGORY: Data on category:\n";
        size_t keys_size =
            static_cast<NVCategory *>(the_column->dtype_info.category)
                ->keys_size();
        if (keys_size > 0) {
          static_cast<NVCategory *>(the_column->dtype_info.category)
              ->get_keys()
              ->print();
        } else {
          std::cout << "Empty!\n";
        }
      } else {
        std::cout << "Category nulled!\n";
      }
      break;
    }
    default: {
      std::cout << "Attempted to print unsupported type.\n";
    }
  }
}

}  // namespace test
}  // namespace blazingdb
#endif  // BLAZINGDB_GPU_FUNCTIONS_H
