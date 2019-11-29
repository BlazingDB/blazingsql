#ifndef TRAITS_RUNTIME_TRAITS
#define TRAITS_RUNTIME_TRAITS

#include <cstdint>
#include <cstddef>
#include <string>
#include <cudf.h>

namespace blazingdb {
namespace test {

inline bool is_dtype_float32(gdf_dtype type) {
    return type == GDF_FLOAT32;
}

inline bool is_dtype_float64(gdf_dtype type) {
    return type == GDF_FLOAT64;
}

inline bool is_dtype_float(gdf_dtype type) {
    return (type == GDF_FLOAT32) || (type == GDF_FLOAT64);
}

inline bool is_dtype_signed(gdf_dtype type) {
    return (type == GDF_INT8    ||
            type == GDF_INT16   ||
            type == GDF_INT32   ||
            type == GDF_INT64   ||
            type == GDF_FLOAT32 ||
            type == GDF_FLOAT64);
}

//TODO felipe percy noboa see upgrade to uints
//inline bool is_dtype_unsigned(gdf_dtype type) {
//    return (type == GDF_UINT8  ||
//            type == GDF_UINT16 ||
//            type == GDF_UINT32 ||
//            type == GDF_UINT64);
//}

inline bool is_dtype_integer(gdf_dtype type) {
    return (type == GDF_INT8   ||
//            type == GDF_UINT8  ||
            type == GDF_INT16  ||
//            type == GDF_UINT16 ||
            type == GDF_INT32  ||
//            type == GDF_UINT32 ||
            type == GDF_INT64//  ||
//            type == GDF_UINT64
			);
}

} // Traits
} // Ral

class gdf_column_cpp;

namespace blazingdb {
namespace test {

    constexpr std::size_t BYTE_SIZE_IN_BITS = 8;

    constexpr std::size_t BITMASK_SIZE_IN_BYTES = 64;


    gdf_size_type get_dtype_size_in_bytes(const gdf_column* column);

    gdf_size_type get_dtype_size_in_bytes(gdf_dtype dtype);


    gdf_size_type get_data_size_in_bytes(const gdf_column_cpp& column);

    gdf_size_type get_data_size_in_bytes(const gdf_column* column);

    gdf_size_type get_data_size_in_bytes(gdf_size_type quantity, gdf_dtype dtype);


    gdf_size_type get_bitmask_size_in_bytes(const gdf_column* column);

    gdf_size_type get_bitmask_size_in_bytes(gdf_size_type quantity);

    gdf_dtype convert_string_dtype(std::string str);

} // namespace traits
} // namespace ral


namespace blazingdb {
namespace test {

namespace {
template <gdf_dtype T>
struct mapDType;

template <>
struct mapDType<GDF_INT8> {
    using type = std::int8_t;
};

template <>
struct mapDType<GDF_INT16> {
    using type = std::int16_t;
};

template <>
struct mapDType<GDF_INT32> {
    using type = std::int32_t;
};

template <>
struct mapDType<GDF_INT64> {
    using type = std::int64_t;
};

template <>
struct mapDType<GDF_FLOAT32> {
    using type = float;
};

template <>
struct mapDType<GDF_FLOAT64> {
    using type = double;
};

template <>
struct mapDType<GDF_DATE32> {
    using type = std::int32_t;
};

template <>
struct mapDType<GDF_DATE64> {
    using type = std::int64_t;
};
} // namespace

template <gdf_dtype T>
using type = typename mapDType<T>::type;


namespace {
template<typename Type>
struct mapType;

template<>
struct mapType<std::int8_t> {
    constexpr static gdf_dtype dtype{GDF_INT8};
};

template<>
struct mapType<std::int16_t> {
    constexpr static gdf_dtype dtype{GDF_INT16};
};

template<>
struct mapType<std::int32_t> {
    constexpr static gdf_dtype dtype{GDF_INT32};
};

template<>
struct mapType<std::int64_t> {
    constexpr static gdf_dtype dtype{GDF_INT64};
};

template<>
struct mapType<float> {
    constexpr static gdf_dtype dtype{GDF_FLOAT32};
};

template<>
struct mapType<double> {
    constexpr static gdf_dtype dtype{GDF_FLOAT64};
};
} // namespace

template <typename Type>
constexpr gdf_dtype dtype = mapType<Type>::dtype;

} // namespace traits
} // namespace ral

#endif
