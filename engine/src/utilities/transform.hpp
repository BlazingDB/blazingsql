#pragma once

#include <cudf/column/column_view.hpp>

namespace ral {
namespace utilities {

/**
 * Used for the substring function in LogicalProject
 */
void inplace_length_to_end_transform(cudf::mutable_column_view& target, const cudf::column_view & start);

}  // namespace utilities
}  // namespace ral
