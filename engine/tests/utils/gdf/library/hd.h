#ifndef RAL_TESTS_UTILS_GDF_HD_H_
#define RAL_TESTS_UTILS_GDF_HD_H_

#include <vector>

#include <GDFColumn.cuh>

namespace gdf {
namespace library {

template <gdf_dtype DTYPE>
struct DTypeTraits {};

#define DTYPE_FACTORY(DTYPE, T)                                                                                        \
	template <>                                                                                                        \
	struct DTypeTraits<GDF_##DTYPE> {                                                                                  \
		typedef T value_type;                                                                                          \
		static constexpr std::size_t size = sizeof(value_type);                                                        \
	}

DTYPE_FACTORY(INT8, std::int8_t);
DTYPE_FACTORY(INT16, std::int16_t);
DTYPE_FACTORY(INT32, std::int32_t);
DTYPE_FACTORY(INT64, std::int64_t);

// TODO percy noboa see upgrade to uints
// DTYPE_FACTORY(UINT8, std::uint8_t);
// DTYPE_FACTORY(UINT16, std::uint16_t);
// DTYPE_FACTORY(UINT32, std::uint32_t);
// DTYPE_FACTORY(UINT64, std::uint64_t);

DTYPE_FACTORY(FLOAT32, float);
DTYPE_FACTORY(FLOAT64, double);
DTYPE_FACTORY(BOOL8, char);
DTYPE_FACTORY(DATE32, std::int32_t);
DTYPE_FACTORY(DATE64, std::int64_t);
DTYPE_FACTORY(TIMESTAMP, std::int64_t);

#undef DTYPE_FACTORY

template <gdf_dtype GDF_DType>
class DType {
public:
	using value_type = typename DTypeTraits<GDF_DType>::value_type;

	static constexpr gdf_dtype value = GDF_DType;
	static constexpr std::size_t size = DTypeTraits<GDF_DType>::size;

	template <class T>
	DType(const T value) : value_{static_cast<value_type>(value)} {}

	operator value_type() const { return value_; }

private:
	const value_type value_;
};

template <gdf_dtype U>
std::vector<typename DType<U>::value_type> HostVectorFrom(const gdf_column_cpp & column_cpp) {
	auto column = const_cast<gdf_column_cpp &>(column_cpp);
	std::vector<typename DType<U>::value_type> vector;
	vector.resize(column.size());
	cudaMemcpy(vector.data(), column.data(), column.size() * DType<U>::size, cudaMemcpyDeviceToHost);
	return vector;
}

std::vector<cudf::valid_type> HostValidFrom(const gdf_column_cpp & column_cpp) {
	auto column = const_cast<gdf_column_cpp &>(column_cpp);
	std::size_t size = column.get_valid_size();

	std::vector<cudf::valid_type> vector;
	vector.resize(size);

	cudaMemcpy(vector.data(), column.valid(), size * sizeof(cudf::valid_type), cudaMemcpyDeviceToHost);

	return vector;
}

}  // namespace library
}  // namespace gdf

#endif
