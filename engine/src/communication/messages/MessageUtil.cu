#include <cudf/copying.hpp>

#include <rmm/exec_policy.hpp>
#include <rmm/cuda_stream_view.hpp>
#include "MessageUtil.cuh"

namespace ral {
namespace communication {
namespace messages {

	std::pair<int32_t, int32_t> getCharsColumnStartAndEnd(const cudf::strings_column_view & column){
		cudf::size_type offset = column.offset();
		cudf::column_view offsets_column = column.offsets();

		int32_t chars_column_start, chars_column_end;
		cudaMemcpy(&chars_column_start, (void*)(offsets_column.head<int32_t>() + offset), sizeof(int32_t), cudaMemcpyDeviceToHost);
		cudaMemcpy(&chars_column_end, (void*)(offsets_column.head<int32_t>() + offset + column.size()), sizeof(int32_t), cudaMemcpyDeviceToHost);
		
		return {chars_column_start, chars_column_end};
	}
	
	std::unique_ptr<cudf::column> getRebasedStringOffsets(const cudf::strings_column_view & column, int32_t chars_column_start){
		cudf::size_type offset = column.offset();
		cudf::column_view offsets_column = column.offsets();

		// NOTE that the offsets column size is usually one more than the number of strings. It starts at 0 and ends at chars_column.size()
		auto new_offsets = cudf::allocate_like(offsets_column, column.size() + 1, cudf::mask_allocation_policy::NEVER);
		auto mutable_col = new_offsets->mutable_view();

		cudf::copy_range_in_place(offsets_column, mutable_col, offset, offset + column.size() + 1, 0);

		rmm::cuda_stream_view stream;

		thrust::transform(rmm::exec_policy(stream),
											mutable_col.begin<int32_t>(),
											mutable_col.end<int32_t>(),
											mutable_col.begin<int32_t>(),
											[chars_column_start] __device__ (int32_t value){
												return value - chars_column_start;
											});

		return new_offsets;
	}

}  // namespace messages
}  // namespace communication
}  // namespace ral
