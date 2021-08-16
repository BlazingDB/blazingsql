#include "GPUComponentMessage.h"

using namespace fmt::literals;

namespace ral {
namespace communication {
namespace messages {

gpu_raw_buffer_container serialize_gpu_message_to_gpu_containers(ral::frame::BlazingTableView table_view){
	std::vector<std::size_t> buffer_sizes;
	std::vector<const char *> raw_buffers;
	std::vector<ColumnTransport> column_offset;
	std::vector<std::unique_ptr<rmm::device_buffer>> temp_scope_holder;
	for(int i = 0; i < table_view.num_columns(); ++i) {
		const cudf::column_view&column = table_view.column(i);
		ColumnTransport col_transport = ColumnTransport{ColumnTransport::MetaData{
															.dtype = (int32_t)column.type().id(),
															.size = column.size(),
															.null_count = column.null_count(),
															.col_name = {},
														},
			.data = -1,
			.valid = -1,
			.strings_data = -1,
			.strings_offsets = -1,
			.strings_nullmask = -1,
			.strings_data_size = 0,
			.strings_offsets_size = 0,
			.size_in_bytes = 0};
		strcpy(col_transport.metadata.col_name, table_view.names().at(i).c_str());

		if (column.size() == 0) {
			// do nothing
		} else if(column.type().id() == cudf::type_id::STRING) {
				cudf::strings_column_view str_col_view{column};

				auto offsets_column = str_col_view.offsets();
				auto chars_column = str_col_view.chars();

				if (str_col_view.size() + 1 == offsets_column.size()){
					// this column does not come from a buffer than had been zero-copy partitioned

					col_transport.strings_data = raw_buffers.size();
					buffer_sizes.push_back(chars_column.size());
					col_transport.size_in_bytes += chars_column.size();
					raw_buffers.push_back(chars_column.head<char>());
					col_transport.strings_data_size = chars_column.size();

					col_transport.strings_offsets = raw_buffers.size();
					col_transport.strings_offsets_size = offsets_column.size() * sizeof(int32_t);
					buffer_sizes.push_back(col_transport.strings_offsets_size);
					col_transport.size_in_bytes += col_transport.strings_offsets_size;
					raw_buffers.push_back(offsets_column.head<char>());

					if(str_col_view.has_nulls()) {
						col_transport.strings_nullmask = raw_buffers.size();
						buffer_sizes.push_back(cudf::bitmask_allocation_size_bytes(str_col_view.size()));
						col_transport.size_in_bytes += cudf::bitmask_allocation_size_bytes(str_col_view.size());
						raw_buffers.push_back((const char *)str_col_view.null_mask());
					}
				} else {
					// this column comes from a column that was zero-copy partitioned

					std::pair<int32_t, int32_t> char_col_start_end = getCharsColumnStartAndEnd(str_col_view);

					std::unique_ptr<CudfColumn> new_offsets = getRebasedStringOffsets(str_col_view, char_col_start_end.first);

					col_transport.strings_data = raw_buffers.size();
					col_transport.strings_data_size = char_col_start_end.second - char_col_start_end.first;
					buffer_sizes.push_back(col_transport.strings_data_size);
					col_transport.size_in_bytes += col_transport.strings_data_size;

					raw_buffers.push_back(chars_column.head<char>() + char_col_start_end.first);
					
					col_transport.strings_offsets = raw_buffers.size();
					col_transport.strings_offsets_size = new_offsets->size() * sizeof(int32_t);
					buffer_sizes.push_back(col_transport.strings_offsets_size);
					col_transport.size_in_bytes += col_transport.strings_offsets_size;

					raw_buffers.push_back(new_offsets->view().head<char>());

					cudf::column::contents new_offsets_contents = new_offsets->release();
					temp_scope_holder.emplace_back(std::move(new_offsets_contents.data));

					if(str_col_view.has_nulls()) {
						col_transport.strings_nullmask = raw_buffers.size();
						buffer_sizes.push_back(cudf::bitmask_allocation_size_bytes(str_col_view.size()));
						col_transport.size_in_bytes += cudf::bitmask_allocation_size_bytes(str_col_view.size());
						temp_scope_holder.emplace_back(std::make_unique<rmm::device_buffer>(
							cudf::copy_bitmask(str_col_view.null_mask(), str_col_view.offset(), str_col_view.offset() + str_col_view.size())));
						raw_buffers.push_back((const char *)temp_scope_holder.back()->data());
					}
				}
		} else {
			col_transport.data = raw_buffers.size();
			buffer_sizes.push_back((std::size_t) column.size() * cudf::size_of(column.type()));
			col_transport.size_in_bytes += (std::size_t) column.size() * cudf::size_of(column.type());

			raw_buffers.push_back(column.head<char>() + column.offset() * cudf::size_of(column.type())); // here we are getting the beginning of the buffer and manually calculating the offset.
			if(column.has_nulls()) {
				col_transport.valid = raw_buffers.size();
				buffer_sizes.push_back(cudf::bitmask_allocation_size_bytes(column.size()));
				col_transport.size_in_bytes += cudf::bitmask_allocation_size_bytes(column.size());
				if (column.offset() == 0){
					raw_buffers.push_back((const char *)column.null_mask());
				} else {
					temp_scope_holder.emplace_back(std::make_unique<rmm::device_buffer>(
						cudf::copy_bitmask(column)));
					raw_buffers.push_back((const char *)temp_scope_holder.back()->data());
				}
			}
		}
		column_offset.push_back(col_transport);
	}
	return std::make_tuple(buffer_sizes, raw_buffers, column_offset, std::move(temp_scope_holder));
}

std::unique_ptr<ral::frame::BlazingHostTable> serialize_gpu_message_to_host_table(ral::frame::BlazingTableView table_view, bool use_pinned) {
	std::vector<std::size_t> buffer_sizes;
	std::vector<const char *> raw_buffers;
	std::vector<ColumnTransport> column_offset;
	std::vector<std::unique_ptr<rmm::device_buffer>> temp_scope_holder;


	std::tie(buffer_sizes, raw_buffers, column_offset, temp_scope_holder) = serialize_gpu_message_to_gpu_containers(table_view);

	
	typedef std::pair< std::vector<ral::memory::blazing_chunked_column_info>, std::vector<std::unique_ptr<ral::memory::blazing_allocation_chunk> >> buffer_alloc_type;
	buffer_alloc_type buffers_and_allocations = ral::memory::convert_gpu_buffers_to_chunks(buffer_sizes,use_pinned);

	
	auto & allocations = buffers_and_allocations.second;
	size_t buffer_index = 0;
	for(auto & chunked_column_info : buffers_and_allocations.first){
		size_t position = 0;
		for(size_t i = 0; i < chunked_column_info.chunk_index.size(); i++){
			size_t chunk_index = chunked_column_info.chunk_index[i];
			size_t offset = chunked_column_info.offset[i];
			size_t chunk_size = chunked_column_info.size[i];
			cudaMemcpyAsync((void *) (allocations[chunk_index]->data + offset), raw_buffers[buffer_index] + position, chunk_size, cudaMemcpyDeviceToHost,0);
			position += chunk_size;
		}		
		buffer_index++;
	}
	cudaStreamSynchronize(0);


	auto table = std::make_unique<ral::frame::BlazingHostTable>(column_offset, std::move(buffers_and_allocations.first),std::move(buffers_and_allocations.second));
	return table;
}

}  // namespace messages
}  // namespace communication
}  // namespace ral
