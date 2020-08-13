#pragma once

#include <spdlog/spdlog.h>
#include <spdlog/async.h>
#include <spdlog/sinks/basic_file_sink.h>
#include <spdlog/sinks/stdout_color_sinks.h>

#include <blazingdb/manager/Context.h>
#include <string>
#include <vector>
#include <tuple>
#include "execution_graph/logic_controllers/LogicPrimitives.h"


namespace ral {
namespace operators {

namespace {
  using blazingdb::manager::Context;
}

std::tuple<std::vector<int>, std::vector<cudf::order>, cudf::size_type>
get_sort_vars(const std::string & query_part);

std::unique_ptr<ral::frame::BlazingTable> sort(const ral::frame::BlazingTableView & table, const std::string & query_part);

std::unique_ptr<ral::frame::BlazingTable> sample(const ral::frame::BlazingTableView & table, const std::string & query_part);

std::unique_ptr<ral::frame::BlazingTable> generate_partition_plan(const std::vector<ral::frame::BlazingTableView> & samples,
	  std::size_t table_num_rows, std::size_t avg_bytes_per_row, const std::string & query_part, Context * context);

std::vector<cudf::table_view> partition_table(const ral::frame::BlazingTableView & partitionPlan, const ral::frame::BlazingTableView & sortedTable, const std::string & query_part);

bool has_limit_only(const std::string & query_part);

int64_t get_limit_rows_when_relational_alg_is_simple(const std::string & query_part);

std::pair<std::unique_ptr<ral::frame::BlazingTable>, int64_t>
limit_table(std::unique_ptr<ral::frame::BlazingTable> table, int64_t num_rows_limit);

std::unique_ptr<ral::frame::BlazingTable> merge(std::vector<ral::frame::BlazingTableView> partitions_to_merge, const std::string & query_part);

}  // namespace operators
}  // namespace ral
