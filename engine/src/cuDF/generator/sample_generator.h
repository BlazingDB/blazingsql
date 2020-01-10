#pragma once

#include "GDFColumn.cuh"
#include <vector>

#include <execution_graph/logic_controllers/LogicPrimitives.h>

namespace cudf {
namespace generator {

gdf_error generate_sample(const std::vector<gdf_column_cpp> & data_frame,
	std::vector<gdf_column_cpp> & sampled_data,
	cudf::size_type num_samples);

std::unique_ptr<ral::frame::BlazingTable> generate_sample(
	const ral::frame::BlazingTableView & blazingTableView, std::size_t num_samples);

}  // namespace generator
}  // namespace cudf
