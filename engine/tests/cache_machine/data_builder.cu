#include "data_builder.h"

 
#include <thrust/sequence.h>
#include <cudf/cudf.h>
#include <cudf/types.hpp>
#include <thrust/device_vector.h>
#include <column/column_factories.hpp>
#include <execution_graph/logic_controllers/LogicPrimitives.h>

#include <from_cudf/cpp_tests/utilities/column_utilities.hpp>
#include <cudf/utilities/type_dispatcher.hpp>
#include <from_cudf/cpp_tests/utilities/column_wrapper.hpp>
#include <from_cudf/cpp_tests/utilities/table_utilities.hpp>

template<class TypeParam>
auto make_col(cudf::size_type size) {
    thrust::device_vector<TypeParam> d_integers(size);
    thrust::sequence( thrust::device, d_integers.begin(), d_integers.end());
    cudf::mask_state state = cudf::ALL_VALID;

    auto integers = cudf::make_numeric_column(cudf::data_type{cudf::experimental::type_to_id<TypeParam>()}, size, state);
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

    auto table = std::make_unique<cudf::experimental::table>(std::move(columns));
    return std::make_unique<ral::frame::BlazingTable>(ral::frame::BlazingTable(std::move(table), column_names));
}
 

std::unique_ptr<ral::frame::BlazingTable>  build_custom_one_column_table() {
    cudf::size_type size = 10;

    auto num_column_1 = make_col<int32_t>(size); 
    std::vector<std::unique_ptr<cudf::column>> columns;
    columns.push_back(std::move(num_column_1));  
    std::vector<std::string> column_names = {"INT64"};

    auto table = std::make_unique<cudf::experimental::table>(std::move(columns));
    return std::make_unique<ral::frame::BlazingTable>(ral::frame::BlazingTable(std::move(table), column_names));

}


//namespace cudf_io = cudf::experimental::io;
//std::shared_ptr<ral::cache::CacheMachine>  createNationMachine() {
//    unsigned long long  gpuMemory = 1024;
//    std::vector<unsigned long long > memoryPerCache = {INT_MAX};
//    std::vector<ral::cache::CacheDataType> cachePolicyTypes = {ral::cache::CacheDataType::LOCAL_FILE};
//    auto source =  std::make_shared<ral::cache::CacheMachine>(gpuMemory, memoryPerCache, cachePolicyTypes);
//     auto PARQUET_NATION_PATH = "/home/aocsa/tpch/DataSet5Part100MB/nation_0_0.parquet";
//    cudf_io::read_parquet_args in_args{cudf_io::source_info{PARQUET_NATION_PATH}};
//    auto table = cudf_io::read_parquet(in_args);
//    std::vector<std::string> column_names= {"n_nationkey", "n_name", "n_regionkey", "n_comment"};
//    auto blazing_table =  std::make_unique<ral::frame::BlazingTable>(std::move(table.tbl), column_names);
//    source->addToCache(std::move(blazing_table));
//    return source;
//}
//
//std::shared_ptr<ral::cache::CacheMachine>  createRegionMachine() {
//    unsigned long long  gpuMemory = 1024;
//    std::vector<unsigned long long > memoryPerCache = {INT_MAX};
//    std::vector<ral::cache::CacheDataType> cachePolicyTypes = {ral::cache::CacheDataType::LOCAL_FILE};
//    auto source =  std::make_shared<ral::cache::CacheMachine>(gpuMemory, memoryPerCache, cachePolicyTypes);
//    auto PARQUET_REGION_PATH = "/home/aocsa/tpch/DataSet5Part100MB/region_0_0.parquet";
//    cudf_io::read_parquet_args in_args{cudf_io::source_info{PARQUET_REGION_PATH}};
//    auto table = cudf_io::read_parquet(in_args);
//    std::vector<std::string> column_names= {"r_regionkey", "r_name", "r_comment"};
//    auto blazing_table =  std::make_unique<ral::frame::BlazingTable>(std::move(table.tbl), column_names);
//
//    source->addToCache(std::move(blazing_table));
//    return source;
//}


