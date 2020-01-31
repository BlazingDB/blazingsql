#pragma once

#include <blazingdb/manager/Context.h>

#include "LogicPrimitives.h"

namespace ral{

namespace processor{

std::unique_ptr<cudf::experimental::table> evaluate_expressions(
    const cudf::table_view & table,
    const std::vector<std::string> & expressions);

std::unique_ptr<ral::frame::BlazingTable> process_project(
  const ral::frame::BlazingTableView & table,
  const std::string & query_part,
  blazingdb::manager::experimental::Context * context);

} // namespace processor
} // namespace ral
