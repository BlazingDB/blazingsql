#ifndef BLAZINGDB_RAL_GROUPBY_OPERATOR_H
#define BLAZINGDB_RAL_GROUPBY_OPERATOR_H

#include "DataFrame.h"
#include <blazingdb/manager/Context.h>
#include <string>
#include <vector>

#include "execution_graph/logic_controllers/LogicPrimitives.h"
#include <cudf/aggregation.hpp>

#include <cudf/column/column_view.hpp>

namespace ral {
namespace operators {



namespace {
using blazingdb::manager::Context;
}  // namespace

bool is_aggregate(std::string query_part);

std::unique_ptr<ral::frame::BlazingTable> process_aggregate(const ral::frame::BlazingTableView & table, std::string query_part, Context * queryContext);

// TODO rommel percy
std::unique_ptr<ral::frame::BlazingTable> groupby_without_aggregations(
	const ral::frame::BlazingTableView & table, const std::vector<int> & group_column_indices);

}  // namespace operators
}  // namespace ral

#endif  // BLAZINGDB_RAL_GROUPBY_OPERATOR_H
