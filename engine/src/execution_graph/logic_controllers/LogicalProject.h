#pragma once

#include "LogicalFilter.h"

namespace ral{

namespace processor{

std::unique_ptr<ral::frame::BlazingTable> process_project(
  const ral::frame::BlazingTableView & table,
  const std::string & query_part,
  blazingdb::manager::Context * context);

} // namespace processor
} // namespace ral