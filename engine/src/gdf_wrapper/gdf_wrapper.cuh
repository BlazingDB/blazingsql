#ifndef _BZ_WRAPPER_H_
#define _BZ_WRAPPER_H_

#include "gdf_types.cuh"
#include "cudf/cudf.h"
#include "cudf/types.h"
#include "cudf/types.hpp"

inline gdf_dtype to_gdf_type(cudf::type_id type_id) {
	switch (type_id) {
		case cudf::type_id::BOOL8: return gdf_dtype::GDF_BOOL8; break;
		case cudf::type_id::INT8: return gdf_dtype::GDF_INT8; break;
		case cudf::type_id::INT16: return gdf_dtype::GDF_INT16; break;
		case cudf::type_id::INT32: return gdf_dtype::GDF_INT32; break;
		case cudf::type_id::INT64: return gdf_dtype::GDF_INT64; break;
		case cudf::type_id::FLOAT32: return gdf_dtype::GDF_FLOAT32; break;
		case cudf::type_id::FLOAT64: return gdf_dtype::GDF_FLOAT64; break;
		case cudf::type_id::STRING: return gdf_dtype::GDF_STRING; break;
		case cudf::type_id::CATEGORY: return gdf_dtype::GDF_STRING_CATEGORY; break;
		case cudf::type_id::TIMESTAMP_DAYS: return gdf_dtype::GDF_DATE32; break;
		case cudf::type_id::TIMESTAMP_SECONDS: return gdf_dtype::GDF_DATE64; break;
		case cudf::type_id::TIMESTAMP_MILLISECONDS: return gdf_dtype::GDF_TIMESTAMP; break;
		// TODO percy cudf0.12 map more types
	}
	
	return gdf_dtype::GDF_invalid;
}

inline cudf::type_id to_type_id(gdf_dtype gdf_type_val) {
	switch (gdf_type_val) {
		case gdf_dtype::GDF_BOOL8: return cudf::type_id::BOOL8; break;
		case gdf_dtype::GDF_INT8: return cudf::type_id::INT8; break;
		case gdf_dtype::GDF_INT16: return cudf::type_id::INT16; break;
		case gdf_dtype::GDF_INT32: return cudf::type_id::INT32; break;
		case gdf_dtype::GDF_INT64: return cudf::type_id::INT64; break;
		case gdf_dtype::GDF_FLOAT32: return cudf::type_id::FLOAT32; break;
		case gdf_dtype::GDF_FLOAT64: return cudf::type_id::FLOAT64; break;
		case gdf_dtype::GDF_STRING: return cudf::type_id::STRING; break;
		case gdf_dtype::GDF_STRING_CATEGORY: return cudf::type_id::CATEGORY; break;
		case gdf_dtype::GDF_DATE32: return cudf::type_id::TIMESTAMP_DAYS; break;
		case gdf_dtype::GDF_DATE64: return cudf::type_id::TIMESTAMP_SECONDS; break;
		case gdf_dtype::GDF_TIMESTAMP: return cudf::type_id::TIMESTAMP_MILLISECONDS; break;
		// TODO percy cudf0.12 map more types
	}
	
	return cudf::type_id::EMPTY;
}

inline gdf_time_unit to_gdf_time_unit(cudf::type_id type_id) {
	switch (type_id) {
		case cudf::type_id::TIMESTAMP_SECONDS: return gdf_time_unit::TIME_UNIT_s; break;
		case cudf::type_id::TIMESTAMP_MILLISECONDS: return gdf_time_unit::TIME_UNIT_ms; break;
		case cudf::type_id::TIMESTAMP_MICROSECONDS: return gdf_time_unit::TIME_UNIT_us; break;
		case cudf::type_id::TIMESTAMP_NANOSECONDS: return gdf_time_unit::TIME_UNIT_ns; break;
	}
	
	return gdf_time_unit::TIME_UNIT_NONE;
}

#endif /* _BZ_WRAPPER_H_ */
