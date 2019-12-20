#include "Traits/RuntimeTraits.h"
#include "GDFColumn.cuh"
#include <cudf.h>

#include <cudf/types.hpp>

namespace ral {
namespace traits {

std::size_t get_data_size_in_bytes(cudf::column_view column) {
	return (column.size()) * cudf::size_of(column.type());
}

gdf_size_type get_data_size_in_bytes(gdf_size_type quantity, cudf::data_type dtype) {
	return quantity * cudf::size_of(dtype);
}

gdf_size_type get_bitmask_size_in_bytes(const gdf_column * column) { return gdf_valid_allocation_size(column->size); }

gdf_size_type get_bitmask_size_in_bytes(gdf_size_type quantity) { return gdf_valid_allocation_size(quantity); }

gdf_dtype convert_string_dtype(std::string str) {
	if(str == "GDF_INT8") {
		return GDF_INT8;
	} else if(str == "GDF_INT16") {
		return GDF_INT16;
	} else if(str == "GDF_INT32") {
		return GDF_INT32;
	} else if(str == "GDF_INT64") {
		return GDF_INT64;
	} else if(str == "GDF_FLOAT32") {
		return GDF_FLOAT32;
	} else if(str == "GDF_FLOAT64") {
		return GDF_FLOAT64;
	} else if(str == "GDF_BOOL8") {
		return GDF_BOOL8;
	} else if(str == "GDF_DATE32") {
		return GDF_DATE32;
	} else if(str == "GDF_DATE64") {
		return GDF_DATE64;
	} else if(str == "GDF_TIMESTAMP") {
		return GDF_TIMESTAMP;
	} else if(str == "GDF_CATEGORY") {
		return GDF_CATEGORY;
	} else if(str == "GDF_STRING") {
		return GDF_STRING;
	} else if(str == "GDF_STRING_CATEGORY") {
		return GDF_STRING_CATEGORY;
	} else {
		return GDF_invalid;
	}
}

}  // namespace traits
}  // namespace ral
