#pragma once

#include <cudf/column/column_view.hpp>

namespace ral {
namespace utilities {

/**
 * Used for the substring function in LogicalProject
 */
void transform_length_to_end(cudf::mutable_column_view& length, const cudf::column_view & start);

/**
 * Used for the substring function in LogicalProject
 */
void transform_start_to_zero_based_indexing(cudf::mutable_column_view& start);

}  // namespace utilities
}  // namespace ral
