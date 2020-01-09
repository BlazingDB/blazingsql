#ifndef BLAZINGDB_RAL_ORDERBY_OPERATOR_H
#define BLAZINGDB_RAL_ORDERBY_OPERATOR_H

#include "DataFrame.h"
#include <blazingdb/manager/Context.h>
#include <string>
#include <vector>
#include "execution_graph/logic_controllers/LogicPrimitives.h"

namespace ral {
namespace operators {

namespace {
using blazingdb::manager::Context;
}  // namespace

bool is_sort(std::string query_part);

void process_sort(blazing_frame & input, std::string query_part, Context * queryContext);

std::unique_ptr<ral::frame::BlazingTable> logicalSort(
  const ral::frame::BlazingTableView & table, std::vector<int> sortColIndices, std::vector<int8_t> sortOrderTypes);

std::unique_ptr<ral::frame::BlazingTable> logicalLimit(
  const ral::frame::BlazingTableView & table, std::string limitRowsStr);

}  // namespace operators
}  // namespace ral

#endif  // BLAZINGDB_RAL_ORDERBY_OPERATOR_H
