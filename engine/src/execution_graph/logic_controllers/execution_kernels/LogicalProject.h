#pragma once

#include <execution_graph/Context.h>
#include "LogicPrimitives.h"
#include "execution_graph/logic_controllers/blazing_table/BlazingColumn.h"

namespace ral{
namespace processor{

/**
 * @brief Evaluates multiple expressions consisting of arithmetic operations and
 * SQL functions.
 *
 * The computation of the results consist of two steps:
 * 1. We evaluate all complex operations operations one by one. Complex operations
 * are operations that can't be mapped as f(input_table[row]) => output_table[row]
 * for a given row in a table e.g. string functions
 *
 * 2. We batch all simple operations and evaluate all of them in a single GPU
 * kernel call. Simple operations are operations that can be mapped as
 * f(input_table[row]) => output_table[row] for a given row in a table e.g.
 * arithmetic operations and cast between primitive types
 */
std::vector<std::unique_ptr<ral::frame::BlazingColumn>> evaluate_expressions(
  const cudf::table_view & table, const std::vector<std::string> & expressions);

std::unique_ptr<ral::frame::BlazingTable> process_project(
  std::unique_ptr<ral::frame::BlazingTable> blazing_table_in,
  const std::string & query_part,
  blazingdb::manager::Context * context);

} // namespace processor
} // namespace ral
