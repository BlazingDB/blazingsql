#ifndef BLAZINGDB_RAL_UTILITIES_COMMONOPERATIONS_H
#define BLAZINGDB_RAL_UTILITIES_COMMONOPERATIONS_H

#include "GDFColumn.cuh"
#include <string>
#include <vector>
#include "execution_graph/logic_controllers/LogicPrimitives.h"


namespace ral {
namespace utilities {

std::vector<gdf_column_cpp> concatTables(const std::vector<std::vector<gdf_column_cpp>> & tables);
std::vector<gdf_column_cpp> normalizeColumnTypes(std::vector<gdf_column_cpp> columns);


}  // namespace utilities
}  // namespace ral

namespace ral {
namespace utilities {
namespace experimental {

using namespace ral::frame;

std::unique_ptr<BlazingTable> concatTables(const std::vector<BlazingTableView> & tables);

std::unique_ptr<ral::frame::BlazingTable> create_empty_table(const std::vector<std::string> &column_names, 
	const std::vector<cudf::type_id> &dtypes, std::vector<size_t> column_indices = std::vector<size_t>());

std::unique_ptr<ral::frame::BlazingTable> create_empty_table(const BlazingTableView & table);

}  // namespace experimental
}  // namespace utilities
}  // namespace ral

#endif  // BLAZINGDB_RAL_COMMONOPERATIONS_H