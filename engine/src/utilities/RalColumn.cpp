#include "utilities/RalColumn.h"

#include "Traits/RuntimeTraits.h"
#include <algorithm>

namespace ral {
namespace utilities {

gdf_column_cpp create_column(const gdf_size_type size, const gdf_dtype dtype, const std::string name) {
	// create gdf_column_cpp
	gdf_column_cpp column;

	// TODO Percy Rommel Jean Pierre improve timestamp resolution
	gdf_dtype_extra_info extra_info;
	extra_info.category = nullptr;
	extra_info.time_unit =
		(dtype == GDF_DATE64 || dtype == GDF_TIMESTAMP ? TIME_UNIT_ms
													   : TIME_UNIT_NONE);  // TODO this should not be hardcoded

	// populate gdf_column_cpp
	auto width = ral::traits::get_dtype_size_in_bytes(dtype);
	column.create_gdf_column(dtype, extra_info, size, nullptr, width, name);

	// done
	return column;
}

gdf_column_cpp create_zero_column(const gdf_size_type size, const gdf_dtype dtype, std::string && name) {
	return create_zero_column(size, dtype, name);
}

gdf_column_cpp create_zero_column(const gdf_size_type size, const gdf_dtype dtype, const std::string & name) {
	// create data array
	std::size_t data_size = ral::traits::get_data_size_in_bytes(size, dtype);
	std::vector<std::uint8_t> data(data_size, 0);

	// create bitmask array
	std::size_t bitmask_size = ral::traits::get_bitmask_size_in_bytes(size);
	std::vector<std::uint8_t> bitmask(bitmask_size, 0);

	// TODO Percy Rommel Jean Pierre improve timestamp resolution
	gdf_dtype_extra_info extra_info;
	extra_info.category = nullptr;
	extra_info.time_unit =
		(dtype == GDF_DATE64 || dtype == GDF_TIMESTAMP ? TIME_UNIT_ms
													   : TIME_UNIT_NONE);  // TODO this should not be hardcoded

	// create gdf_column_cpp
	gdf_column_cpp column;
	auto width = ral::traits::get_dtype_size_in_bytes(dtype);
	column.create_gdf_column(dtype, extra_info, size, data.data(), bitmask.data(), width, name);

	// done
	return column;
}

cudf::table create_table(const std::vector<gdf_column_cpp> & columns) {
	std::vector<gdf_column *> columns_ptrs(columns.size());
	std::transform(columns.begin(), columns.end(), columns_ptrs.begin(), [&](const gdf_column_cpp & el) {
		return el.get_gdf_column();
	});
	cudf::table table(columns_ptrs);
	return table;
}

}  // namespace utilities
}  // namespace ral
