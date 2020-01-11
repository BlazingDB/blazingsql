#include "CalciteExpressionParsing.h"
#include "cuDF/generator/random_generator.cuh"
#include "cuDF/generator/sample_generator.h"
#include "cuDF/safe_nvcategory_gather.hpp"
#include "utilities/RalColumn.h"
#include <cudf/legacy/copying.hpp>
#include <cudf/legacy/table.hpp>
#include <types.hpp>

#include <cudf/copying.hpp>

namespace cudf {
namespace generator {

gdf_error generate_sample(const std::vector<gdf_column_cpp> & data_frame,
	std::vector<gdf_column_cpp> & sampled_data,
	cudf::size_type num_samples) {
	if(data_frame.size() == 0) {
		return GDF_DATASET_EMPTY;
	}

	if(num_samples <= 0) {
		sampled_data = data_frame;
		return GDF_SUCCESS;
	}

	cudf::generator::RandomVectorGenerator<int32_t> generator(0L, data_frame[0].get_gdf_column()->size());
	std::vector<int32_t> arrayIdx = generator(num_samples);

	// Gather
	sampled_data.clear();
	sampled_data.resize(data_frame.size());
	for(size_t i = 0; i < data_frame.size(); i++) {
		auto & input_col = data_frame[i];
		if(input_col.get_gdf_column()->has_nulls())
			sampled_data[i].create_gdf_column(input_col.get_gdf_column()->type().id(),
				arrayIdx.size(),
				nullptr,
				ral::traits::get_dtype_size_in_bytes(input_col.get_gdf_column()->type().id()),
				input_col.name());
		else
			sampled_data[i].create_gdf_column(input_col.get_gdf_column()->type().id(),
				arrayIdx.size(),
				nullptr,
				nullptr,
				ral::traits::get_dtype_size_in_bytes(input_col.get_gdf_column()->type().id()),
				input_col.name());
	}

	cudf::table srcTable = ral::utilities::create_table(data_frame);
	cudf::table destTable = ral::utilities::create_table(sampled_data);

	gdf_column_cpp gatherMap;
	gatherMap.create_gdf_column(cudf::type_id::INT32,
		arrayIdx.size(),
		arrayIdx.data(),
		ral::traits::get_dtype_size_in_bytes(cudf::type_id::INT32),
		"");

	// TODO percy cudf0.12 port to cudf::column	and custrings
	//    cudf::gather(&srcTable, (gdf_size_type*)(gatherMap.get_gdf_column()->data), &destTable);
	//    ral::init_string_category_if_null(destTable);

	return GDF_SUCCESS;
}

std::unique_ptr<ral::frame::BlazingTable> generate_sample(
	const ral::frame::BlazingTableView & blazingTableView, std::size_t num_samples) {
	CudfTableView view = blazingTableView.view();

	if(0 == view.num_columns()) {
		throw std::length_error("Without columns");
	}

	cudf::size_type num_rows = view.num_rows();
	if(0 == num_rows) {
		throw std::length_error("Without rows");
	}

	if(view.num_rows() <= num_samples) {
		throw std::out_of_range("Invalid number of samples");
	}

	cudf::generator::RandomVectorGenerator<std::int32_t> generator(0L, num_rows);
	std::vector<std::int32_t> arrayIdx = generator(num_samples);

	rmm::device_buffer gatherData(arrayIdx.data(), num_samples * sizeof(std::int32_t));

	cudf::column gatherMap{cudf::data_type{cudf::type_id::INT32}, num_samples, gatherData};

	std::unique_ptr<CudfTable> sampleTable =
		cudf::experimental::gather(view, gatherMap.view(), true, rmm::mr::get_default_resource());

	return std::make_unique<ral::frame::BlazingTable>(std::move(sampleTable), blazingTableView.names());
}

}  // namespace generator
}  // namespace cudf
