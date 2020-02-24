#include "column_factory.h"
#include <thrust/sequence.h>
#include <from_cudf/cpp_tests/utilities/column_utilities.hpp>
#include <cudf/utilities/type_dispatcher.hpp>
#include <from_cudf/cpp_tests/utilities/type_lists.hpp>
#include <from_cudf/cpp_tests/utilities/column_wrapper.hpp>
#include <from_cudf/cpp_tests/utilities/legacy/cudf_test_utils.cuh>
#include <from_cudf/cpp_tests/utilities/table_utilities.hpp>

namespace blazingdb {
namespace test {
 
 template<class TypeParam>
 auto make_col(cudf::size_type size) {
    thrust::device_vector<TypeParam> d_integers(size);
    thrust::sequence( thrust::device, d_integers.begin(), d_integers.end());
    cudf::mask_state state = cudf::mask_state::ALL_VALID;

    auto integers = cudf::make_numeric_column(cudf::data_type{cudf::experimental::type_to_id<TypeParam>()}, size, state);
    auto integers_view = integers->mutable_view();
    cudaMemcpy( integers_view.data<TypeParam>(), d_integers.data().get(), size * sizeof(TypeParam), cudaMemcpyDeviceToDevice );
    return integers;
 }



ral::frame::BlazingTable build_custom_table() {
    cudf::size_type size = 10;

    auto num_column_1 = make_col<int32_t>(size); 
    auto num_column_2 = make_col<int64_t>(size);
    auto num_column_3 = make_col<float>(size); 
    auto num_column_4 = make_col<double>(size);

	std::vector<std::unique_ptr<cudf::column>> columns;
    columns.push_back(std::move(num_column_1));
    columns.push_back(std::move(num_column_2));
    columns.push_back(std::move(num_column_3));
    columns.push_back(std::move(num_column_4));


    cudf::test::strings_column_wrapper col2({"d", "e", "a", "d", "k", "d", "l", "a", "b", "c"}, {1, 0, 1, 1, 1, 1, 1, 1, 0, 1});

	std::unique_ptr<cudf::column> str_col = std::make_unique<cudf::column>(std::move(col2));
    columns.push_back(std::move(str_col));

	std::vector<std::string> column_names = {"INT64", "INT32", "FLOAT64", "FLOAT32", "STRING"};

    auto table = std::make_unique<cudf::experimental::table>(std::move(columns));
	return ral::frame::BlazingTable(std::move(table), column_names);
}



} // 
} //