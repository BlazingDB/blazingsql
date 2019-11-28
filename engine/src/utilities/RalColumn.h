#ifndef BLAZINGDB_RAL_UTILITIES_RALCOLUMN_H
#define BLAZINGDB_RAL_UTILITIES_RALCOLUMN_H

#include "GDFColumn.cuh"
#include "Traits/RuntimeTraits.h"
#include <cudf/legacy/table.hpp>

namespace ral {
namespace utilities {

template <typename ColumnType>
gdf_column_cpp create_column(const std::vector<ColumnType>& data, const gdf_dtype dtype, std::string&& name) {
    return create_column<ColumnType>(data, dtype, name);
}

template <typename ColumnType>
gdf_column_cpp create_column(const std::vector<ColumnType>& data, const gdf_dtype dtype, const std::string& name = "") {
    // create data array
    std::size_t data_size = ral::traits::get_data_size_in_bytes(data.size(), dtype);

    // create bitmask array
    std::size_t bitmask_size = ral::traits::get_bitmask_size_in_bytes(data.size());
    std::vector<std::uint8_t> bitmask(bitmask_size, 0xFF);

    // create gdf_column_cpp
    gdf_column_cpp column;
    auto width = ral::traits::get_dtype_size_in_bytes(dtype);
    column.create_gdf_column(dtype, data.size(), const_cast<ColumnType*>(data.data()), bitmask.data(), width, name);

    // done
    return column;
}

gdf_column_cpp create_column(const gdf_size_type size, const gdf_dtype dtype, const std::string name = "");

gdf_column_cpp create_zero_column(const gdf_size_type size, const gdf_dtype dtype, std::string&& name);

gdf_column_cpp create_zero_column(const gdf_size_type size, const gdf_dtype dtype, const std::string& name = "");

cudf::table create_table(const std::vector<gdf_column_cpp> & columns);

} // namespace utilities
} // namespace ral

#endif //BLAZINGDB_RAL_RALCOLUMN_H
