#ifndef _BLAZING_DEBUG_UTILS_H
#define _BLAZING_DEBUG_UTILS_H

#include "execution_graph/logic_controllers/LogicPrimitives.h"
// #include "execution_graph/logic_controllers/CacheMachine.h"

namespace ral {
namespace utilities {

std::string type_string(cudf::data_type dtype);

void print_blazing_table_view(ral::frame::BlazingTableView table_view, const std::string table_name="");

void print_blazing_table_view_schema(ral::frame::BlazingTableView table_view, const std::string table_name="");

std::string blazing_table_view_schema_to_string(ral::frame::BlazingTableView table_view, const std::string table_name);

// std::string cache_data_schema_to_string(ral::cache::CacheData * cache_data);

}  // namespace utilities
}  // namespace ral

#endif  //_BLAZING_DEBUG_UTILS_H
