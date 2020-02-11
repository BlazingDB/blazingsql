#pragma once

#include <memory>

#include "execution_graph/logic_controllers/LogicPrimitives.h"

namespace ral {
namespace generator {

std::unique_ptr<ral::frame::BlazingTable> generate_sample(const ral::frame::BlazingTableView & blazingTableView, size_t num_samples);

} // namespace generator
} // namespace ral
