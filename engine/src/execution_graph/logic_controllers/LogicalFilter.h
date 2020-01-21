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

std::unique_ptr<cudf::column> evaluate_expression(
  const cudf::table_view & table,
  const std::string & expression,
  cudf::data_type output_type);

std::unique_ptr<ral::frame::BlazingTable> process_filter(
  const ral::frame::BlazingTableView & table,
  const std::string & query_part,
  blazingdb::manager::experimental::Context * context);

std::vector<std::unique_ptr<ral::frame::BlazingTable> > hashPartition(
    const ral::frame::BlazingTableView & table,
    std::vector<cudf::size_type> const& columns_to_hash,
    int numPartitions);

std::unique_ptr<ral::frame::BlazingTable> processJoin(
  const ral::frame::BlazingTableView & table_left,
  const ral::frame::BlazingTableView & table_right,
  const std::string & expression);


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
