#pragma once

#include "GDFColumn.cuh"
#include "Traits/RuntimeTraits.h"

namespace ral {
namespace test {

namespace {

template <typename ColumnType>
std::vector<ColumnType> create_data(std::initializer_list<ColumnType> & list) {
	return std::vector<ColumnType>(list.begin(), list.end());
}

std::vector<cudf::valid_type> create_bitmask_set(cudf::size_type size) {
	cudf::size_type bitmask_size = ral::traits::get_bitmask_size_in_bytes(size);
	return std::vector<cudf::valid_type>(bitmask_size, 0xFF);
}

}  // namespace

template <gdf_dtype T>
gdf_column_cpp create_column(std::initializer_list<ral::traits::type<T>> list) {
	cudf::size_type size = list.size();
	auto data = create_data(list);
	auto bitmask = create_bitmask_set(size);

	gdf_column_cpp output;
	output.create_gdf_column(T, size, data.data(), bitmask.data(), ral::traits::get_dtype_size_in_bytes(T));
	return output;
}

template <gdf_dtype T>
gdf_column_cpp create_column(std::vector<ral::traits::type<T>> list) {
	cudf::size_type size = list.size();
	auto bitmask = create_bitmask_set(size);

	gdf_column_cpp output;
	output.create_gdf_column(T, size, list.data(), bitmask.data(), ral::traits::get_dtype_size_in_bytes(T));
	return output;
}

template <gdf_dtype T>
std::vector<gdf_column_cpp> create_table(std::vector<std::vector<ral::traits::type<T>>> & input) {
	std::vector<gdf_column_cpp> table;

	for(auto & data : input) {
		table.emplace_back(create_column<T>(data));
	}

	return table;
}

}  // namespace test
}  // namespace ral
