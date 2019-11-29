#include "tests/utilities/gdf_column_cpp_utilities.h"
#include <vector>
#include <random>
#include <algorithm>
#include <thrust/equal.h>

namespace ral {
namespace test {

gdf_column_cpp create_gdf_column_cpp(std::size_t size, gdf_dtype dtype) {
    // create random generator
    std::random_device random_device;
    std::default_random_engine random_engine(random_device());
    std::uniform_int_distribution<std::uint8_t> generator;

    // create lambda generator
    auto Generator = [=]() mutable {
        return (std::uint8_t)generator(random_engine);
    };

    // create data array
    std::size_t data_size = ral::traits::get_data_size_in_bytes(size, dtype);
    std::vector<std::uint8_t> data;
    data.resize(data_size);

    // create valid array
    std::size_t valid_size = ral::traits::get_bitmask_size_in_bytes(size);
    std::vector<std::uint8_t> valid;
    valid.resize(valid_size);

    // populate arrays
    std::generate_n(data.data(), data_size, Generator);
    std::generate_n(valid.data(), valid_size, Generator);

    // create gdf_column_cpp
    gdf_column_cpp column;
    auto width = ral::traits::get_dtype_size_in_bytes(dtype);
	gdf_dtype_extra_info extra_info{TIME_UNIT_NONE};
    column.create_gdf_column(dtype, extra_info, size, data.data(), valid.data(), width);

    // done
    return column;
}

gdf_column_cpp create_null_gdf_column_cpp(std::size_t size, gdf_dtype dtype) {
    // create data array
    std::size_t data_size = ral::traits::get_data_size_in_bytes(size, dtype);
    std::vector<std::uint8_t> data(data_size, 0);

    // create valid array
    std::size_t valid_size = ral::traits::get_bitmask_size_in_bytes(size);
    std::vector<std::uint8_t> valid(valid_size, 0);

    // create gdf_column_cpp
    gdf_column_cpp column;
    auto width = ral::traits::get_dtype_size_in_bytes(dtype);
    gdf_dtype_extra_info extra_info{TIME_UNIT_NONE};
    column.create_gdf_column(dtype, extra_info, size, data.data(), valid.data(), width);

    // done
    return column;
}

std::vector<std::uint8_t> get_column_data(gdf_column* column) {
    std::vector<std::uint8_t> result;

    std::size_t data_size = ral::traits::get_data_size_in_bytes(column);
    result.resize(data_size);
    cudaMemcpy(result.data(), column->data, data_size, cudaMemcpyDeviceToHost);

    return result;
}

std::vector<std::uint8_t> get_column_valid(gdf_column* column) {
    std::vector<std::uint8_t> result;

    std::size_t valid_size = ral::traits::get_bitmask_size_in_bytes(column);
    result.resize(valid_size);
    cudaMemcpy(result.data(), column->valid, valid_size, cudaMemcpyDeviceToHost);

    return result;
}

} // namespace test
} // namespace ral


bool operator==(const gdf_column& lhs, const gdf_column& rhs) {
    if (lhs.size != rhs.size) {
        return false;
    }
    if (lhs.dtype != rhs.dtype) {
        return false;
    }
    if (lhs.null_count != rhs.null_count) {
        return false;
    }
    if (lhs.dtype_info.time_unit != rhs.dtype_info.time_unit) {
        return false;
    }

    if (!(lhs.data && rhs.data)) {
        return false;
    }

    auto data_size = ral::traits::get_data_size_in_bytes(&lhs);
    if (!thrust::equal(thrust::cuda::par,
                       reinterpret_cast<const std::uint8_t*>(lhs.data),
                       reinterpret_cast<const std::uint8_t*>(lhs.data) + data_size,
                       reinterpret_cast<const std::uint8_t*>(rhs.data))) {
        return false;
    }

    if (!(lhs.valid && rhs.valid)) {
        return false;
    }

    auto valid_size = ral::traits::get_bitmask_size_in_bytes(&rhs);
    if (!thrust::equal(thrust::cuda::par,
                       reinterpret_cast<const std::uint8_t*>(lhs.valid),
                       reinterpret_cast<const std::uint8_t*>(lhs.valid) + valid_size,
                       reinterpret_cast<const std::uint8_t*>(rhs.valid))) {
        return false;
    }

    return true;
}

bool operator==(const gdf_column_cpp& lhs, const gdf_column_cpp& rhs) {
    if (lhs.is_ipc() != rhs.is_ipc()) {
        return false;
    }
    if (lhs.get_column_token() != rhs.get_column_token()) {
        return false;
    }
    return *(lhs.get_gdf_column()) == *(rhs.get_gdf_column());
}
