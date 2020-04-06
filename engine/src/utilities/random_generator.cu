#include <cudf/copying.hpp>
#include <thrust/random.h>
#include <vector>

#include "random_generator.cuh"

namespace ral {
namespace generator {
namespace {

template <typename T>
struct UniformRandomGenerator {
    UniformRandomGenerator(T min_value, T max_value, unsigned int time_value)
    : min_value{min_value}, max_value{max_value}, time_value{time_value}
    { }

    __host__ __device__
    unsigned int hash(unsigned int index, unsigned int min_value, unsigned int max_value) {
        return ((time_value * 96377 + (index + 1) * 95731 + (min_value + 1) * 96517 + (max_value + 1) * 99991) % 1073676287);
    }

    __host__ __device__
    T operator()(std::size_t index) {
        thrust::default_random_engine random_engine(hash(index, min_value, max_value));
        thrust::random::uniform_int_distribution<T> distribution(min_value, max_value - 1);
        random_engine.discard(index);
        return distribution(random_engine);
    }

    const T min_value;
    const T max_value;
    const unsigned int time_value;
};


template <typename T>
class RandomVectorGenerator {
public:
    RandomVectorGenerator(T min_value, T max_value)
    : min_value{min_value}, max_value{max_value}
    { }

public:
    std::vector<T> operator()(std::size_t size) {
        rmm::device_vector<T> data(size);

        thrust::transform(thrust::counting_iterator<T>(0),
                          thrust::counting_iterator<T>(size),
                          data.begin(),
                          UniformRandomGenerator<T>(min_value, max_value, 1));

        std::vector<T> result(size);
        thrust::copy(data.begin(), data.end(), result.begin());

        return result;
    }

private:
    const T min_value;
    const T max_value;
};

} // namespace

std::unique_ptr<ral::frame::BlazingTable> generate_sample(
	const ral::frame::BlazingTableView & blazingTableView, std::size_t num_samples){
	CudfTableView view = blazingTableView.view();

	cudf::size_type num_rows = view.num_rows();

	RandomVectorGenerator<std::int32_t> generator(0L, num_rows);
	std::vector<std::int32_t> arrayIdx = generator(num_samples);
	
	rmm::device_buffer gatherData(arrayIdx.data(), num_samples * sizeof(std::int32_t));

	cudf::column gatherMap{cudf::data_type{cudf::type_id::INT32}, num_samples, std::move(gatherData)};

	std::unique_ptr<CudfTable> sampleTable =
		cudf::experimental::gather(view, gatherMap.view(), true, rmm::mr::get_default_resource());

	return std::make_unique<ral::frame::BlazingTable>(std::move(sampleTable), blazingTableView.names());
}

} // namespace generator
} // namespace cudf
