#ifndef TRAITS_RUNTIME_TRAITS
#define TRAITS_RUNTIME_TRAITS

#include <cstddef>
#include <cstdint>
#include <cudf/types.h>
#include <cudf/types.hpp>
#include <string>

namespace Ral {
namespace Traits {

inline bool is_dtype_float32(cudf::type_id type) { return type == cudf::type_id::FLOAT32; }

inline bool is_dtype_float64(cudf::type_id type) { return type == cudf::type_id::FLOAT64; }

inline bool is_dtype_float(cudf::type_id type) { return (type == cudf::type_id::FLOAT32) || (type == cudf::type_id::FLOAT64); }

inline bool is_dtype_signed(cudf::type_id type) {
	return (type == cudf::type_id::INT8 || type == cudf::type_id::INT16 || type == cudf::type_id::INT32 || type == cudf::type_id::INT64 || type == cudf::type_id::FLOAT32 ||
			type == cudf::type_id::FLOAT64);
}

// TODO felipe percy noboa see upgrade to uints
// inline bool is_dtype_unsigned(gdf_dtype type) {
//    return (type == GDF_UINT8  ||
//            type == GDF_UINT16 ||
//            type == GDF_UINT32 ||
//            type == GDF_UINT64);
//}

inline bool is_dtype_integer(cudf::type_id type) {
	return (type == cudf::type_id::INT8 ||
			//            type == GDF_UINT8  ||
			type == cudf::type_id::INT16 ||
			//            type == GDF_UINT16 ||
			type == cudf::type_id::INT32 ||
			//            type == GDF_UINT32 ||
			type == cudf::type_id::INT64  //  ||
							   //            type == GDF_UINT64
	);
}

}  // namespace Traits
}  // namespace Ral

class gdf_column_cpp;

namespace ral {
namespace traits {

cudf::size_type get_dtype_size_in_bytes(cudf::column * column);

cudf::size_type get_dtype_size_in_bytes(cudf::type_id dtype);

cudf::size_type get_data_size_in_bytes(cudf::column * column);

cudf::size_type get_data_size_in_bytes(cudf::size_type quantity, cudf::type_id dtype);


cudf::size_type get_bitmask_size_in_bytes(cudf::column * column);

cudf::size_type get_bitmask_size_in_bytes(cudf::size_type quantity);

}  // namespace traits
}  // namespace ral

#endif
