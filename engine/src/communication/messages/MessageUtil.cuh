#pragma once

#include <vector>
#include <tuple>
#include <memory>
#include <rmm/device_buffer.hpp>
#include <cudf/column/column_view.hpp>

#include "execution_graph/logic_controllers/LogicPrimitives.h"



namespace ral {
namespace communication {
namespace messages {
namespace experimental {

	std::pair<cudf::size_type, cudf::size_type> getCharsColumnStartAndEnd(const CudfColumnView & column);
	
	std::unique_ptr<CudfColumn> getRebasedStringOffsets(const CudfColumnView & column, cudf::size_type chars_column_start);

}  // namespace experimental
}  // namespace messages
}  // namespace communication
}  // namespace ral