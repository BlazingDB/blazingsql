#ifndef _BLAZING_DEBUG_UTILS_H
#define _BLAZING_DEBUG_UTILS_H

#include <from_cudf/cpp_tests/utilities/column_utilities.hpp>
#include "execution_graph/logic_controllers/LogicPrimitives.h"

namespace ral {
namespace utilities {

void print_blazing_table_view(ral::frame::BlazingTableView table_view, const std::string table_name="");

void print_blazing_table_view_schema(ral::frame::BlazingTableView table_view, const std::string table_name="");

}  // namespace utilities
}  // namespace ral

#endif  //_BLAZING_DEBUG_UTILS_H