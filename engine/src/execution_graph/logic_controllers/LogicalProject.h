#pragma once

#include <execution_graph/Context.h>
#include "LogicPrimitives.h"
#include "execution_graph/logic_controllers/BlazingColumn.h"

namespace ral{
namespace processor{

std::vector<std::unique_ptr<ral::frame::BlazingColumn>> evaluate_expressions(
  const cudf::table_view & table, const std::vector<std::string> & expressions);

std::unique_ptr<ral::frame::BlazingTable> process_project(
  std::unique_ptr<ral::frame::BlazingTable> blazing_table_in,
  const std::string & query_part,
  blazingdb::manager::Context * context);

} // namespace processor
} // namespace ral
