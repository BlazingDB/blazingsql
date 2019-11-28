#pragma once

#include <vector>
#include "GDFColumn.cuh"

namespace cudf {
namespace generator {

gdf_error generate_sample(const std::vector<gdf_column_cpp>& data_frame,
                          std::vector<gdf_column_cpp>& sampled_data,
                          gdf_size_type num_samples);

} // namespace generator
} // namespace cudf
