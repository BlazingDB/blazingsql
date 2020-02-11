#ifndef _BZ_WRAPPER_H_
#define _BZ_WRAPPER_H_

#include "cudf/cudf.h"
#include "cudf/types.h"
#include "cudf/types.hpp"
#include <cudf/scalar/scalar.hpp>
#include <cudf/scalar/scalar_factories.hpp>
#include <cudf/utilities/type_dispatcher.hpp>
#include <cudf/scalar/scalar_device_view.cuh>

// TODO: delete this gdf_dtype is the legacy API
inline cudf::type_id to_type_id(gdf_dtype gdf_type_val) {
	switch (gdf_type_val) {
		case gdf_dtype::GDF_BOOL8: return cudf::type_id::BOOL8; 
		case gdf_dtype::GDF_INT8: return cudf::type_id::INT8; 
		case gdf_dtype::GDF_INT16: return cudf::type_id::INT16;
		case gdf_dtype::GDF_INT32: return cudf::type_id::INT32;
		case gdf_dtype::GDF_INT64: return cudf::type_id::INT64;
		case gdf_dtype::GDF_FLOAT32: return cudf::type_id::FLOAT32;
		case gdf_dtype::GDF_FLOAT64: return cudf::type_id::FLOAT64;
		case gdf_dtype::GDF_STRING: return cudf::type_id::STRING;
		case gdf_dtype::GDF_DATE32: return cudf::type_id::TIMESTAMP_DAYS;
		case gdf_dtype::GDF_DATE64: return cudf::type_id::TIMESTAMP_SECONDS;
		case gdf_dtype::GDF_TIMESTAMP: return cudf::type_id::TIMESTAMP_MILLISECONDS;
		// TODO percy cudf0.12 map more types
		case gdf_dtype::GDF_invalid:
		case gdf_dtype::GDF_CATEGORY:
		case gdf_dtype::N_GDF_TYPES: throw std::runtime_error("Unsupported gdf dtypes");
	}

	return cudf::type_id::EMPTY;
}

#endif /* _BZ_WRAPPER_H_ */
