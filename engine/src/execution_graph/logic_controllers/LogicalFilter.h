#pragma once

#include "LogicPrimitives.h"
#include "blazingdb/manager/Context.h"
#include <utility>

namespace ral{

namespace processor{



/**
Takes a table and applies a boolean filter to it
*/
std::unique_ptr<ral::frame::BlazingTable> applyBooleanFilter(
  const ral::frame::BlazingTableView & table,
  const CudfColumnView boolValues);

std::vector<std::unique_ptr<ral::frame::BlazingTable> > hashPartition(
    const ral::frame::BlazingTableView & table,
    std::vector<cudf::size_type> const& columns_to_hash,
    int numPartitions);




std::unique_ptr<ral::frame::BlazingTable> evaluateExpression(
  const ral::frame::BlazingTableView & table,
  const std::string & expression);

std::unique_ptr<ral::frame::BlazingTable> process_logical_join(blazingdb::manager::experimental::Context * context,
      const ral::frame::BlazingTableView & table_left,
      const ral::frame::BlazingTableView & table_right,
      const std::string & expression);

std::unique_ptr<ral::frame::BlazingTable> processJoin(
  const ral::frame::BlazingTableView & table_left,
  const ral::frame::BlazingTableView & table_right,
  const std::string & expression);

std::pair<std::unique_ptr<ral::frame::BlazingTable>, std::unique_ptr<ral::frame::BlazingTable> >  process_distribution(
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


}

}
