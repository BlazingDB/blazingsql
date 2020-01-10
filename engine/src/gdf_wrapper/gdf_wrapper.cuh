#ifndef _BZ_WRAPPER_H_
#define _BZ_WRAPPER_H_

#include "gdf_types.cuh"
#include "cudf/cudf.h"
#include "cudf/types.h"
#include "cudf/types.hpp"
#include <cudf/scalar/scalar.hpp>
#include <cudf/scalar/scalar_factories.hpp>
#include <cudf/utilities/type_dispatcher.hpp>
#include <cudf/scalar/scalar_device_view.cuh>

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
		case cudf::type_id::EMPTY:
		case cudf::type_id::TIMESTAMP_MICROSECONDS:
		case cudf::type_id::TIMESTAMP_NANOSECONDS:
		case cudf::type_id::NUM_TYPE_IDS: throw std::runtime_error("Unsupported cudf type ids");
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
		case gdf_dtype::GDF_invalid:
		case gdf_dtype::GDF_CATEGORY:
		case gdf_dtype::N_GDF_TYPES: throw std::runtime_error("Unsupported gdf dtypes");
	}

	return cudf::type_id::EMPTY;
}

inline gdf_time_unit to_gdf_time_unit(cudf::type_id type_id) {
	switch (type_id) {
		case cudf::type_id::TIMESTAMP_SECONDS: return gdf_time_unit::TIME_UNIT_s; break;
		case cudf::type_id::TIMESTAMP_MILLISECONDS: return gdf_time_unit::TIME_UNIT_ms; break;
		case cudf::type_id::TIMESTAMP_MICROSECONDS: return gdf_time_unit::TIME_UNIT_us; break;
		case cudf::type_id::TIMESTAMP_NANOSECONDS: return gdf_time_unit::TIME_UNIT_ns; break;
		// TODO: support types
		case cudf::type_id::EMPTY:
		case cudf::type_id::INT8:
		case cudf::type_id::INT16:
		case cudf::type_id::INT32:
		case cudf::type_id::INT64:
		case cudf::type_id::FLOAT32:
		case cudf::type_id::FLOAT64:
		case cudf::type_id::BOOL8:
		case cudf::type_id::TIMESTAMP_DAYS:
		case cudf::type_id::CATEGORY:
		case cudf::type_id::STRING:
		case cudf::type_id::NUM_TYPE_IDS: throw std::runtime_error("Unsupported cudf type ids for time unit");
	}

	return gdf_time_unit::TIME_UNIT_NONE;
}

inline gdf_scalar to_gdf_scalar(const std::unique_ptr<cudf::scalar> &s) {
	// TODO percy cudf0.12 maybe we don't need at all to imlement this wrapper since gdf_scalar is gonna be obsolete anyways
	switch (s->type().id()) {
		case cudf::type_id::BOOL8: {
			break;
		}
		case cudf::type_id::INT8: {
			break;
		}
		case cudf::type_id::INT16: {
			break;
		}
		case cudf::type_id::INT32: {
			break;
		}
		case cudf::type_id::INT64: {
			break;
		}
		case cudf::type_id::FLOAT32: {
			break;
		}
		case cudf::type_id::FLOAT64: {
			break;
		}
		case cudf::type_id::STRING: {
			break;
		}
		case cudf::type_id::CATEGORY: {
			break;
		}
		case cudf::type_id::TIMESTAMP_DAYS: {
			break;
		}
		case cudf::type_id::TIMESTAMP_SECONDS: {
			break;
		}
		case cudf::type_id::TIMESTAMP_MILLISECONDS: {
			break;
		}
		// TODO percy cudf0.12 map more types	}
		case cudf::type_id::EMPTY:
		case cudf::type_id::TIMESTAMP_MICROSECONDS:
		case cudf::type_id::TIMESTAMP_NANOSECONDS:
		case cudf::type_id::NUM_TYPE_IDS: throw std::runtime_error("Unsupported cudf type ids for scalar");
	}

	return gdf_scalar();
}

#endif /* _BZ_WRAPPER_H_ */
