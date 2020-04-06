#pragma once

#include <blazingdb/manager/Context.h>

#include "LogicPrimitives.h"
#include "execution_graph/logic_controllers/BlazingColumn.h"

namespace ral{

namespace processor{

std::vector<std::unique_ptr<ral::frame::BlazingColumn>> evaluate_expressions(
  std::vector<std::unique_ptr<ral::frame::BlazingColumn>> blazing_columns_in, const std::vector<std::string> & expressions);

std::unique_ptr<ral::frame::BlazingTable> process_project(
  std::unique_ptr<ral::frame::BlazingTable> blazing_table_in,
  const std::string & query_part,
  blazingdb::manager::experimental::Context * context);

} // namespace processor
} // namespace ral
