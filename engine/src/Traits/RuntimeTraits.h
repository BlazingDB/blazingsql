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

constexpr std::size_t BYTE_SIZE_IN_BITS = 8;

constexpr std::size_t BITMASK_SIZE_IN_BYTES = 64;


cudf::size_type get_dtype_size_in_bytes(cudf::column * column);

cudf::size_type get_dtype_size_in_bytes(cudf::type_id dtype);

cudf::size_type get_data_size_in_bytes(cudf::column * column);

cudf::size_type get_data_size_in_bytes(cudf::size_type quantity, cudf::type_id dtype);


cudf::size_type get_bitmask_size_in_bytes(cudf::column * column);

cudf::size_type get_bitmask_size_in_bytes(cudf::size_type quantity);

cudf::type_id convert_string_dtype(std::string str);

}  // namespace traits
}  // namespace ral


namespace ral {
namespace traits {

namespace {
template <cudf::type_id T>
struct mapDType;

template <>
struct mapDType<cudf::type_id::INT8> {
	using type = std::int8_t;
};

template <>
struct mapDType<cudf::type_id::INT16> {
	using type = std::int16_t;
};

template <>
struct mapDType<cudf::type_id::INT32> {
	using type = std::int32_t;
};

template <>
struct mapDType<cudf::type_id::INT64> {
	using type = std::int64_t;
};

template <>
struct mapDType<cudf::type_id::FLOAT32> {
	using type = float;
};

template <>
struct mapDType<cudf::type_id::FLOAT64> {
	using type = double;
};

// TODO percy cudf0.12 by default timestamp for bz is MS but we need to use proper time resolution
template <>
struct mapDType<cudf::type_id::TIMESTAMP_DAYS> {
	using type = gdf_date32;
};

template <>
struct mapDType<cudf::type_id::TIMESTAMP_SECONDS> {
	using type = gdf_date64;
};
}  // namespace

template <cudf::type_id T>
using type = typename mapDType<T>::type;


namespace {
template <typename Type>
struct mapType;

template <>
struct mapType<std::int8_t> {
	constexpr static cudf::type_id dtype{cudf::type_id::INT8};
};

template <>
struct mapType<std::int16_t> {
	constexpr static cudf::type_id dtype{cudf::type_id::INT16};
};

template <>
struct mapType<std::int32_t> {
	constexpr static cudf::type_id dtype{cudf::type_id::INT32};
};

template <>
struct mapType<std::int64_t> {
	constexpr static cudf::type_id dtype{cudf::type_id::INT64};
};

template <>
struct mapType<float> {
	constexpr static cudf::type_id dtype{cudf::type_id::FLOAT32};
};

template <>
struct mapType<double> {
	constexpr static cudf::type_id dtype{cudf::type_id::FLOAT64};
};
}  // namespace

template <typename Type>
constexpr cudf::type_id dtype = mapType<Type>::dtype;

}  // namespace traits
}  // namespace ral

#endif
