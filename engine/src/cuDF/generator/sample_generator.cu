#include "cuDF/generator/sample_generator.h"
#include "cuDF/generator/random_generator.cuh"
#include "CalciteExpressionParsing.h"
#include "utilities/RalColumn.h"
#include <cudf/legacy/copying.hpp>
#include <types.hpp>
#include <cudf/legacy/table.hpp>
#include "cuDF/safe_nvcategory_gather.hpp"

namespace cudf {
namespace generator {

gdf_error generate_sample(const std::vector<gdf_column_cpp>& data_frame,
                          std::vector<gdf_column_cpp>& sampled_data,
                          cudf::size_type num_samples) {
    if (data_frame.size() == 0) {
        return GDF_DATASET_EMPTY;
    }

    if (num_samples <= 0) {
        sampled_data = data_frame;
        return GDF_SUCCESS;
    }

    cudf::generator::RandomVectorGenerator<int32_t> generator(0L, data_frame[0].size());
    std::vector<int32_t> arrayIdx = generator(num_samples);

    // Gather
    sampled_data.clear();
    sampled_data.resize(data_frame.size());
    for(size_t i = 0; i < data_frame.size(); i++) {
		auto& input_col = data_frame[i];
		if (input_col.valid())
			sampled_data[i].create_gdf_column(input_col.dtype(), input_col.dtype_info(), arrayIdx.size(), nullptr, ral::traits::get_dtype_size_in_bytes(input_col.dtype()), input_col.name());
		else
			sampled_data[i].create_gdf_column(input_col.dtype(), input_col.dtype_info(), arrayIdx.size(), nullptr, nullptr, ral::traits::get_dtype_size_in_bytes(input_col.dtype()), input_col.name());
    }

    cudf::table srcTable = ral::utilities::create_table(data_frame);
    cudf::table destTable = ral::utilities::create_table(sampled_data);

    gdf_column_cpp gatherMap;
    gatherMap.create_gdf_column(cudf::type_id::INT32, gdf_dtype_extra_info{TIME_UNIT_NONE,nullptr}, arrayIdx.size(), arrayIdx.data(), ral::traits::get_dtype_size_in_bytes(cudf::type_id::INT32), "");

    cudf::gather(&srcTable, (gdf_index_type*)(gatherMap.get_gdf_column()->data), &destTable);
    ral::init_string_category_if_null(destTable);

    return GDF_SUCCESS;
}

} // namespace generator
} // namespace cudf
