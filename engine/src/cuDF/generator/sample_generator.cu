#include "CalciteExpressionParsing.h"
#include "cuDF/generator/random_generator.cuh"
#include "cuDF/generator/sample_generator.h"
#include "cuDF/safe_nvcategory_gather.hpp"
#include <types.hpp>

#include <cudf/copying.hpp>

namespace cudf {
namespace generator {

std::unique_ptr<ral::frame::BlazingTable> generate_sample(
	const ral::frame::BlazingTableView & blazingTableView, std::size_t num_samples) {
	CudfTableView view = blazingTableView.view();

	cudf::size_type num_rows = view.num_rows();

	cudf::generator::RandomVectorGenerator<std::int32_t> generator(0L, num_rows);
	std::vector<std::int32_t> arrayIdx = generator(num_samples);
	
	rmm::device_buffer gatherData(arrayIdx.data(), num_samples * sizeof(std::int32_t));

	cudf::column gatherMap{cudf::data_type{cudf::type_id::INT32}, num_samples, std::move(gatherData)};

	std::unique_ptr<CudfTable> sampleTable =
		cudf::experimental::gather(view, gatherMap.view(), true, rmm::mr::get_default_resource());

	return std::make_unique<ral::frame::BlazingTable>(std::move(sampleTable), blazingTableView.names());
}

}  // namespace generator
}  // namespace cudf
