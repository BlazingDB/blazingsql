#include <cudf/null_mask.hpp>
#include <cudf/column/column_factories.hpp>
#include <cstring>

#include "serializer.hpp"
#include "communication/messages/MessageUtil.cuh"

namespace comm {

std::unique_ptr<ral::frame::BlazingTable> deserialize_from_gpu_raw_buffers(
	const std::vector<blazingdb::transport::ColumnTransport> & columns_offsets,
	std::vector<rmm::device_buffer> & raw_buffers,
	cudaStream_t stream) {
	size_t num_columns = columns_offsets.size();
	std::vector<std::unique_ptr<cudf::column>> received_samples(num_columns);
	std::vector<std::string> column_names(num_columns);


	assert(raw_buffers.size() >= 0);

	for(size_t i = 0; i < num_columns; ++i) {
		auto data_offset = columns_offsets[i].data;
		auto string_offset = columns_offsets[i].strings_data;
		if(string_offset != -1) {
			cudf::size_type num_strings = columns_offsets[i].metadata.size;
			std::unique_ptr<cudf::column> offsets_column =
				std::make_unique<cudf::column>(cudf::data_type{cudf::type_id::INT32},
					num_strings + 1,
					std::move(raw_buffers[columns_offsets[i].strings_offsets]));

			cudf::size_type total_bytes = columns_offsets[i].strings_data_size;
			std::unique_ptr<cudf::column> chars_column =
				std::make_unique<cudf::column>(cudf::data_type{cudf::type_id::INT8},
					total_bytes,
					std::move(raw_buffers[columns_offsets[i].strings_data]));
			rmm::device_buffer null_mask;
			if(columns_offsets[i].strings_nullmask != -1)
				null_mask = std::move(raw_buffers[columns_offsets[i].strings_nullmask]);

			cudf::size_type null_count = columns_offsets[i].metadata.null_count;
			auto unique_column = cudf::make_strings_column(
				num_strings, std::move(offsets_column), std::move(chars_column), null_count, std::move(null_mask));
			received_samples[i] = std::move(unique_column);

		} else {
			cudf::data_type dtype = cudf::data_type{cudf::type_id(columns_offsets[i].metadata.dtype)};
			cudf::size_type column_size = (cudf::size_type) columns_offsets[i].metadata.size;

			if(columns_offsets[i].valid != -1) {
				// this is a valid
				auto valid_offset = columns_offsets[i].valid;
				auto unique_column = std::make_unique<cudf::column>(
					dtype, column_size, std::move(raw_buffers[data_offset]), std::move(raw_buffers[valid_offset]));
				received_samples[i] = std::move(unique_column);
			} else if(data_offset != -1) {
				auto unique_column =
					std::make_unique<cudf::column>(dtype, column_size, std::move(raw_buffers[data_offset]));
				received_samples[i] = std::move(unique_column);
			} else {
				auto unique_column = cudf::make_empty_column(dtype);
				received_samples[i] = std::move(unique_column);
			}
		}
		column_names[i] = std::string{columns_offsets[i].metadata.col_name};
	}

	auto unique_table = std::make_unique<cudf::table>(std::move(received_samples));

	return std::make_unique<ral::frame::BlazingTable>(std::move(unique_table), column_names);
}

}  // namespace comm
