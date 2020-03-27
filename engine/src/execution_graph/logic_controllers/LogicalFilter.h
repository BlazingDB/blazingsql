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
  blazingdb::manager::experimental::Context * context);

std::unique_ptr<ral::frame::BlazingTable> process_join(const ral::frame::BlazingTableView & table_left,
															   const ral::frame::BlazingTableView & table_right,
															   const std::string & expression,
															   blazingdb::manager::experimental::Context * context);

void parseJoinConditionToColumnIndices(const std::string & condition, std::vector<int> & columnIndices);

std::unique_ptr<ral::frame::BlazingTable> process_logical_join(blazingdb::manager::experimental::Context * context,
      const ral::frame::BlazingTableView & table_left,
      const ral::frame::BlazingTableView & table_right,
      const std::string & expression);

std::unique_ptr<ral::frame::BlazingTable> processJoin(
  const ral::frame::BlazingTableView & table_left,
  const ral::frame::BlazingTableView & table_right,
  const std::string & expression);

std::pair<std::unique_ptr<ral::frame::BlazingTable>, std::unique_ptr<ral::frame::BlazingTable> >  process_optimal_inner_join_distribution(
    const ral::frame::BlazingTableView & left,
    const ral::frame::BlazingTableView & right,
    const std::string & query,
    blazingdb::manager::experimental::Context * context);
/**
Should be able to perform all 3 types of aggregations only locally what comes in
as a solid chunk, inputs should be columns to aggregate, aggregation type,
columns to group by
*/
std::unique_ptr<ral::frame::BlazingTable> computeAggregation(
    const ral::frame::BlazingTableView & table
  /* inputs you need to run different aggregations*/);

std::unique_ptr<ral::frame::BlazingTable> process_distribution_table(
  	const ral::frame::BlazingTableView & table,
    std::vector<int> & columnIndices,
    blazingdb::manager::experimental::Context * context);

} // namespace processor
} // namespace ral
