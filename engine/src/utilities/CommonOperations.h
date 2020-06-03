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

std::unique_ptr<cudf::table> create_empty_table(const std::vector<cudf::type_id> &dtypes);

std::unique_ptr<ral::frame::BlazingTable> create_empty_table(const BlazingTableView & table);

cudf::data_type get_common_type(cudf::data_type type1, cudf::data_type type2, bool strict);

std::vector<cudf::data_type> get_common_types(const std::vector<cudf::data_type> & types1, const std::vector<cudf::data_type> & types2, bool strict);

void normalize_types(std::unique_ptr<ral::frame::BlazingTable> & table,  const std::vector<cudf::data_type> & types,
	 		std::vector<cudf::size_type> column_indices = std::vector<cudf::size_type>() );

int64_t get_table_size_bytes(const ral::frame::BlazingTableView & table);

}  // namespace utilities
}  // namespace ral
