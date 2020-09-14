#pragma once

#include <blazingdb/manager/Context.h>

#include "LogicPrimitives.h"

namespace ral{

namespace processor{

bool is_logical_filter(const std::string & query_part);

/**
Takes a table and applies a boolean filter to it
*/
std::unique_ptr<ral::frame::BlazingTable> applyBooleanFilter(
  const ral::frame::BlazingTableView & table,
  const CudfColumnView & boolValues);

std::unique_ptr<ral::frame::BlazingTable> process_filter(
  const ral::frame::BlazingTableView & table,
  const std::string & query_part,
  blazingdb::manager::Context * context);

bool check_if_has_nulls(CudfTableView const& input, std::vector<cudf::size_type> const& keys);

} // namespace processor
} // namespace ral
