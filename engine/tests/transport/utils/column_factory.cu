#include "column_factory.h"
#include <thrust/sequence.h>

namespace blazingdb {
namespace test {


rmm::device_vector<thrust::pair<const char*,cudf::size_type>> create_test_string () {
    std::vector<const char*> h_test_strings{ "col1",
                                             "col2",
                                             "", 
											 "", // todo: nullptr
											 "col5",
											 "col6",
                                             "col7",
                                             "", 
											 "", // todo: nullptr
											 "col10" };
    cudf::size_type memsize = 0;
    for( auto itr=h_test_strings.begin(); itr!=h_test_strings.end(); ++itr )
        memsize += *itr ? (cudf::size_type)strlen(*itr) : 0;
    cudf::size_type count = (cudf::size_type)h_test_strings.size();
    thrust::host_vector<char> h_buffer(memsize);
    thrust::device_vector<char> d_buffer(memsize);
    thrust::host_vector<thrust::pair<const char*,cudf::size_type> > strings(count);
    thrust::host_vector<cudf::size_type> h_offsets(count+1);
    cudf::size_type offset = 0;
    cudf::size_type nulls = 0;
    h_offsets[0] = 0;
    for( cudf::size_type idx=0; idx < count; ++idx )
    {
        const char* str = h_test_strings[idx];
        if( !str )
        {
            strings[idx] = thrust::pair<const char*,cudf::size_type>{nullptr,0};
            nulls++;
        }
        else
        {
            cudf::size_type length = (cudf::size_type)strlen(str);
            memcpy( h_buffer.data() + offset, str, length );
            strings[idx] = thrust::pair<const char*,cudf::size_type>{d_buffer.data().get()+offset,length};
            offset += length;
        }
        h_offsets[idx+1] = offset;
    }

    rmm::device_vector<thrust::pair<const char*,cudf::size_type>> d_strings(strings);
    cudaMemcpy( d_buffer.data().get(), h_buffer.data(), memsize, cudaMemcpyHostToDevice );

    return d_strings;
}
 
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

ral::frame::BlazingTable build_custom_table() {
    cudf::size_type size = 10;
	cudf::mask_state state = cudf::ALL_VALID;
    rmm::device_vector<thrust::pair<const char*,cudf::size_type>> d_strings = create_test_string();
    auto str_column = cudf::make_strings_column(d_strings);

    auto num_column_1 = make_col<int32_t>(size); 
    auto num_column_2 = make_col<int64_t>(size);
    auto num_column_3 = make_col<float>(size); 
    auto num_column_4 = make_col<double>(size);

	std::vector<std::unique_ptr<cudf::column>> columns;
    columns.push_back(std::move(num_column_1));
    columns.push_back(std::move(num_column_2));
    columns.push_back(std::move(num_column_3));
    columns.push_back(std::move(num_column_4));
	columns.push_back(std::move(str_column));
	
	std::vector<std::string> column_names = {"INT64", "INT32", "FLOAT64", "FLOAT32", "STRING"};

    auto table = std::make_unique<cudf::experimental::table>(std::move(columns));
	return ral::frame::BlazingTable(std::move(table), column_names);
}



} // 
} //