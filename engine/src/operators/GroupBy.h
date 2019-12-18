#ifndef BLAZINGDB_RAL_GROUPBY_OPERATOR_H
#define BLAZINGDB_RAL_GROUPBY_OPERATOR_H

#include "DataFrame.h"
#include <blazingdb/manager/Context.h>
#include <string>
#include <vector>

namespace ral {
namespace operators {

namespace {
using blazingdb::manager::Context;
}  // namespace

bool is_aggregate(std::string query_part);

void process_aggregate(blazing_frame & input, std::string query_part, Context * queryContext);

std::vector<gdf_column_cpp> groupby_without_aggregations(
	std::vector<gdf_column_cpp> & input, const std::vector<int> & group_column_indices);

}  // namespace operators
}  // namespace ral

#endif  // BLAZINGDB_RAL_GROUPBY_OPERATOR_H
