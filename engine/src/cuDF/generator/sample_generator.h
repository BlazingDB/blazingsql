#pragma once

#include <vector>

#include <execution_graph/logic_controllers/LogicPrimitives.h>

namespace cudf {
namespace generator {

std::unique_ptr<ral::frame::BlazingTable> generate_sample(
	const ral::frame::BlazingTableView & blazingTableView, std::size_t num_samples);

}  // namespace generator
}  // namespace cudf
