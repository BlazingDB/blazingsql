
#include "MessageUtil.cuh"

#include <cudf/copying.hpp>
#include <cudf/column/column.hpp>
#include <cudf/column/column_view.hpp>
#include <cudf/column/column_device_view.cuh>
#include <thrust/transform.h>

#include <from_cudf/cpp_tests/utilities/column_utilities.hpp>


namespace ral {
namespace communication {
namespace messages {
namespace experimental {

	std::pair<cudf::size_type, cudf::size_type> getCharsColumnStartAndEnd(const CudfColumnView & column){
		
		cudf::size_type offset = column.offset();
		CudfColumnView offsets_column = column.child(0);

		cudf::size_type chars_column_start, chars_column_end;
		cudaMemcpy(&chars_column_start, (void*)(offsets_column.begin<cudf::size_type>() + offset), sizeof(cudf::size_type), cudaMemcpyDeviceToHost);
		cudaMemcpy(&chars_column_end, (void*)(offsets_column.begin<cudf::size_type>() + offset + column.size()), sizeof(cudf::size_type), cudaMemcpyDeviceToHost);
		return std::make_pair(chars_column_start, chars_column_end);
	}
	
	std::unique_ptr<CudfColumn> getRebasedStringOffsets(const CudfColumnView & column, cudf::size_type chars_column_start){

		CudfColumnView offsets_column = column.child(0);

		// NOTE that the offsets column size is usually one more than the number of strings. It starts at 0 and ends at chars_column.size()
		cudf::size_type offset = column.offset();
		std::unique_ptr<CudfColumn> new_offsets = cudf::experimental::allocate_like(offsets_column, 
				column.size() + 1, cudf::experimental::mask_allocation_policy::NEVER);

		auto mutable_col = new_offsets->mutable_view();

		cudf::experimental::copy_range_in_place(offsets_column, mutable_col,
				offset, offset + column.size() + 1, 0);

		struct subtracting_operator	{
			cudf::size_type _sub;
			subtracting_operator(cudf::size_type sub){
				_sub = sub;
			}
			__host__ __device__
			cudf::size_type operator()(const cudf::size_type x)
			{
				return (x - _sub);
			}
		};

		subtracting_operator op(chars_column_start);
		auto start_src = mutable_col.begin<cudf::size_type>();
		auto end_src = mutable_col.end<cudf::size_type>();
		auto start_dst = mutable_col.begin<cudf::size_type>();		
		thrust::transform(start_src, end_src, start_dst, op);

		return new_offsets;
	}

}  // namespace experimental
}  // namespace messages
}  // namespace communication
}  // namespace ral