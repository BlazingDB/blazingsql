#ifndef BLAZINGDB_RAL_UTILITIES_COMMONOPERATIONS_H
#define BLAZINGDB_RAL_UTILITIES_COMMONOPERATIONS_H

#include <vector>
#include <string>
#include "GDFColumn.cuh"


namespace ral {
namespace utilities {

std::vector<gdf_column_cpp> concatTables(const std::vector<std::vector<gdf_column_cpp>>& tables);
std::vector<gdf_column_cpp> normalizeColumnTypes(std::vector<gdf_column_cpp> columns);


} // namespace utilities
} // namespace ral

#endif //BLAZINGDB_RAL_COMMONOPERATIONS_H