#include "Traits/RuntimeTraits.h"
#include "GDFColumn.cuh"
#include <cudf.h>

namespace ral {
namespace traits {

cudf::size_type get_dtype_size_in_bytes(cudf::type_id dtype) {
	cudf::size_type size = 0;
	switch(dtype) {
	case cudf::type_id::BOOL8: size = sizeof(gdf_bool8); break;
	case cudf::type_id::INT8: size = sizeof(int8_t); break;
	case cudf::type_id::INT16: size = sizeof(int16_t); break;
	case cudf::type_id::INT32: size = sizeof(int32_t); break;
	case cudf::type_id::INT64: size = sizeof(int64_t); break;
	case cudf::type_id::FLOAT32: size = sizeof(float); break;
	case cudf::type_id::FLOAT64: size = sizeof(double); break;
	case cudf::type_id::TIMESTAMP_DAYS: size = sizeof(gdf_date32); break;
	case cudf::type_id::TIMESTAMP_SECONDS: size = sizeof(gdf_date64); break;
	// TODO percy cudf0.12 by default timestamp for bz is MS but we need to use proper time resolution
	case cudf::type_id::TIMESTAMP_MILLISECONDS: size = sizeof(gdf_timestamp); break;
	case cudf::type_id::CATEGORY: size = sizeof(gdf_category); break;
	case cudf::type_id::STRING: size = sizeof(gdf_nvstring_category); break;
	default: size = 0; break;
	}
	return size;
}

cudf::size_type get_dtype_size_in_bytes(cudf::column * column) { return get_dtype_size_in_bytes(column->type().id()); }

cudf::size_type get_dtype_size_in_bytes(cudf::column_view & column) { return get_dtype_size_in_bytes(column.type().id()); }

cudf::size_type get_data_size_in_bytes(cudf::column * column) {
	return (column->size() * get_dtype_size_in_bytes(column->type().id()));
}

cudf::size_type get_data_size_in_bytes(cudf::size_type quantity, cudf::type_id dtype) {
	return (quantity * get_dtype_size_in_bytes(dtype));
}

cudf::size_type get_bitmask_size_in_bytes(cudf::column * column) { return gdf_valid_allocation_size(column->size()); }

cudf::size_type get_bitmask_size_in_bytes(cudf::size_type quantity) { return gdf_valid_allocation_size(quantity); }

cudf::type_id convert_string_dtype(std::string str) {
	if(str == "GDF_INT8") {
		return cudf::type_id::INT8;
	} else if(str == "GDF_INT16") {
		return cudf::type_id::INT16;
	} else if(str == "GDF_INT32") {
		return cudf::type_id::INT32;
	} else if(str == "GDF_INT64") {
		return cudf::type_id::INT64;
	} else if(str == "GDF_FLOAT32") {
		return cudf::type_id::FLOAT32;
	} else if(str == "GDF_FLOAT64") {
		return cudf::type_id::FLOAT64;
	} else if(str == "GDF_BOOL8") {
		return cudf::type_id::BOOL8;
	} else if(str == "GDF_DATE32") {
	// TODO percy cudf0.12 by default timestamp for bz is MS but we need to use proper time resolution
		return cudf::type_id::TIMESTAMP_DAYS;
	} else if(str == "GDF_DATE64") {
		return cudf::type_id::TIMESTAMP_SECONDS;
	} else if(str == "GDF_TIMESTAMP") {
		// TODO percy cudf0.12 by default timestamp for bz is MS but we need to use proper time resolution
		return cudf::type_id::TIMESTAMP_MILLISECONDS;
	} else if(str == "GDF_CATEGORY") {
		return cudf::type_id::CATEGORY;
	} else if(str == "GDF_STRING") {
		return cudf::type_id::STRING;
	// TODO percy cudf0.12 custrings this was not commented
//	} else if(str == "GDF_STRING_CATEGORY") {
//		return GDF_STRING_CATEGORY;
	} else {
		// TODO percy cudf0.12 was invalid here, should we consider empty?
		return cudf::type_id::EMPTY;
	}
}

}  // namespace traits
}  // namespace ral
