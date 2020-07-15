#include <tests/utilities/base_fixture.hpp>
#include <tests/utilities/column_utilities.hpp>
#include <tests/utilities/column_wrapper.hpp>
#include <tests/utilities/type_lists.hpp>

#include <execution_graph/logic_controllers/LogicPrimitives.h>

#include "utilities/random_generator.cuh"
#include "tests/utilities/BlazingUnitTest.h"

template <class T>
class SampleGeneratorTest : public BlazingUnitTest {};

using NumericTypesForSampling = cudf::test::Types<int8_t, int16_t, int32_t, int64_t, float, double>;

TYPED_TEST_CASE(SampleGeneratorTest, NumericTypesForSampling);

TYPED_TEST(SampleGeneratorTest, BaseCase) {
	using T =  TypeParam;

	cudf::test::fixed_width_column_wrapper<TypeParam> column1{{T(0), T(1), T(2), T(3), T(4), T(5)}, {1, 1, 1, 1, 1, 1}};
	cudf::test::fixed_width_column_wrapper<TypeParam> column2{{T(4), T(5), T(6), T(7), T(8), T(9)}, {1, 1, 1, 1, 1, 1}};

	CudfTableView cudfTableView{{column1, column2}};

	const std::vector<std::string> columnNames{"column1", "column2"};
	ral::frame::BlazingTableView blazingTableView{cudfTableView, columnNames};

	std::unique_ptr<ral::frame::BlazingTable> sampleTable = ral::generator::generate_sample(blazingTableView, 4);

	CudfTableView sampleView = sampleTable->view();

	EXPECT_EQ(2, sampleView.num_columns());
	EXPECT_EQ(4, sampleView.num_rows());

	std::cout << "c1: " << cudf::test::to_string(sampleView.column(0), " ") << std::endl;
	std::cout << "c2: " << cudf::test::to_string(sampleView.column(1), " ") << std::endl;

	// check samples

	auto host = cudf::test::to_host<TypeParam>(sampleView.column(0));

	for(std::size_t i = 0; i < host.first.size(); i++) {
		EXPECT_THAT(host.first[i], testing::AllOf(testing::Ge(0), testing::Le(5)));
	}

	host = cudf::test::to_host<TypeParam>(sampleView.column(1));

	for(std::size_t i = 0; i < host.first.size(); i++) {
		EXPECT_THAT(host.first[i], testing::AllOf(testing::Ge(4), testing::Le(9)));
	}
}

class SampleGeneratorExceptionsTest : public BlazingUnitTest {};

TEST_F(SampleGeneratorExceptionsTest, WithoutColumns) {
	std::vector<CudfColumnView> columns;
	CudfTableView cudfTableView{columns};

	ral::frame::BlazingTableView blazingTableView{cudfTableView, {}};

	EXPECT_THROW(ral::generator::generate_sample(blazingTableView, 4), std::logic_error);
}

TEST_F(SampleGeneratorExceptionsTest, WithoutRows) {
	cudf::test::fixed_width_column_wrapper<std::int32_t> column{};

	CudfTableView cudfTableView{{column}};

	ral::frame::BlazingTableView blazingTableView{cudfTableView, {}};

	EXPECT_THROW(ral::generator::generate_sample(blazingTableView, 4), std::logic_error);
}
