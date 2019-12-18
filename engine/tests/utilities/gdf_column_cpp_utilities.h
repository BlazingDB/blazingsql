#pragma once

#include "GDFColumn.cuh"
#include "Traits/RuntimeTraits.h"

namespace ral {
namespace test {

gdf_column_cpp create_gdf_column_cpp(std::size_t size, gdf_dtype dtype);

gdf_column_cpp create_null_gdf_column_cpp(std::size_t size, gdf_dtype dtype);

template <typename TypeColumn>
std::vector<TypeColumn> get_column_data(const gdf_column_cpp & column) {
	std::vector<TypeColumn> result(column.size());

	std::size_t data_size = ral::traits::get_data_size_in_bytes(column);
	cudaMemcpy(result.data(), column.data(), data_size, cudaMemcpyDeviceToHost);

	return result;
}

std::vector<std::uint8_t> get_column_data(gdf_column * column);

std::vector<std::uint8_t> get_column_valid(gdf_column * column);

}  // namespace test
}  // namespace ral

bool operator==(const gdf_column & lhs, const gdf_column & rhs);

bool operator==(const gdf_column_cpp & lhs, const gdf_column_cpp & rhs);
