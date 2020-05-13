#pragma once

#include <string>
#include <vector>
#include "execution_graph/logic_controllers/LogicPrimitives.h"

namespace ral {
namespace utilities {

using namespace ral::frame;

std::unique_ptr<BlazingTable> concatTables(const std::vector<BlazingTableView> & tables);

std::unique_ptr<ral::frame::BlazingTable> create_empty_table(const std::vector<std::string> &column_names,
	const std::vector<cudf::type_id> &dtypes, std::vector<size_t> column_indices = std::vector<size_t>());

std::unique_ptr<cudf::experimental::table> create_empty_table(const std::vector<cudf::type_id> &dtypes);

std::unique_ptr<ral::frame::BlazingTable> create_empty_table(const BlazingTableView & table);

std::vector<std::unique_ptr<ral::frame::BlazingColumn>> normalizeColumnTypes(std::vector<std::unique_ptr<ral::frame::BlazingColumn>> columns);

int64_t get_table_size_bytes(const ral::frame::BlazingTableView & table);

}  // namespace utilities
}  // namespace ral
