
#include "execution_graph/logic_controllers/LogicalProject.h"
#include "execution_graph/logic_controllers/CacheMachine.h"
#include <cudf/cudf.h>
#include <from_cudf/cpp_tests/utilities/base_fixture.hpp>
#include <src/execution_graph/logic_controllers/LogicalFilter.h>
#include <src/from_cudf/cpp_tests/utilities/column_wrapper.hpp>
#include <src/utilities/DebuggingUtils.h>
#include "../BlazingUnitTest.h"

using blazingdb::manager::Context;
using blazingdb::transport::Address;
using blazingdb::transport::Node;

struct CacheMachineTest : public BlazingUnitTest {
	CacheMachineTest() {}
	~CacheMachineTest() {}
};

template<class TypeParam>
std::unique_ptr<cudf::column> make_col(cudf::size_type size) {
	thrust::device_vector<TypeParam> d_integers(size);
	thrust::sequence( thrust::device, d_integers.begin(), d_integers.end());
	cudf::mask_state state = cudf::mask_state::ALL_VALID;

	auto integers = cudf::make_numeric_column(cudf::data_type{cudf::type_to_id<TypeParam>()}, size, state);
	auto integers_view = integers->mutable_view();
	cudaMemcpy( integers_view.data<TypeParam>(), d_integers.data().get(), size * sizeof(TypeParam), cudaMemcpyDeviceToDevice );
	return integers;
}

std::unique_ptr<ral::frame::BlazingTable> build_custom_table() {
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

	auto table = std::make_unique<cudf::table>(std::move(columns));
	return std::make_unique<ral::frame::BlazingTable>(ral::frame::BlazingTable(std::move(table), column_names));
}


std::unique_ptr<ral::frame::BlazingTable>  build_custom_one_column_table() {
	cudf::size_type size = 10;

	auto num_column_1 = make_col<int32_t>(size);
	std::vector<std::unique_ptr<cudf::column>> columns;
	columns.push_back(std::move(num_column_1));
	std::vector<std::string> column_names = {"INT64"};

	auto table = std::make_unique<cudf::table>(std::move(columns));
	return std::make_unique<ral::frame::BlazingTable>(ral::frame::BlazingTable(std::move(table), column_names));

}

TEST_F(CacheMachineTest, CacheMachineTest) {
	ral::cache::CacheMachine cacheMachine;

	for(int i = 0; i < 10; ++i) {
		auto table = build_custom_table();
		std::cout << ">> " << i << "|" << table->sizeInBytes() << std::endl;
		cacheMachine.addToCache(std::move(table));
		if(i % 5 == 0) {
			auto cacheTable = cacheMachine.pullFromCache();
		}
	}
	std::this_thread::sleep_for(std::chrono::seconds(1));
}
