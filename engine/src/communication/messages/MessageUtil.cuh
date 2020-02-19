#pragma once

#include <cudf/column/column.hpp>
#include <cudf/column/column_view.hpp>
#include <cudf/strings/strings_column_view.hpp>
#include <utility>
#include <memory>

namespace ral {
namespace communication {
namespace messages {
namespace experimental {

	std::pair<int32_t, int32_t> getCharsColumnStartAndEnd(const cudf::strings_column_view & column);
	
	std::unique_ptr<cudf::column> getRebasedStringOffsets(const cudf::strings_column_view & column, int32_t chars_column_start);

}  // namespace experimental
}  // namespace messages
}  // namespace communication
}  // namespace ral
