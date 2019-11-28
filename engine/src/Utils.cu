#include "Utils.cuh"
#include "cuDF/Allocator.h"
#include <thrust/device_vector.h>

void gdf_sequence(int32_t* data, size_t size, int32_t init_val){
  auto d_ptr = thrust::device_pointer_cast(data);
  thrust::sequence(d_ptr, d_ptr + size, init_val);
}

void gdf_sequence(int32_t* data, size_t size, int32_t init_val, int32_t step){
  auto d_ptr = thrust::device_pointer_cast(data);
  thrust::sequence(d_ptr, d_ptr + size, init_val, step);
}

NVCategory* repeated_string_category(std::string str, size_t repeat_times){
  const char* pStr = str.c_str();
  const char** pStrs = &pStr;
  NVCategory* category = NVCategory::create_from_array(pStrs, 1);

  thrust::device_vector<int> device_vector(repeat_times, 0);
  NVCategory* resultCategory = category->gather_and_remap(thrust::raw_pointer_cast(device_vector.data().get()), repeat_times);
  
  NVCategory::destroy(category);

  return resultCategory;
}
