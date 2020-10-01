#include "GPUComponentMessage.h"

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

std::unique_ptr<ral::frame::BlazingHostTable> serialize_gpu_message_to_host_table(ral::frame::BlazingTableView table_view) {
	std::vector<std::size_t> buffer_sizes;
	std::vector<const char *> raw_buffers;
	std::vector<ColumnTransport> column_offset;
	std::vector<std::unique_ptr<rmm::device_buffer>> temp_scope_holder;
	std::tie(buffer_sizes, raw_buffers, column_offset, temp_scope_holder) = serialize_gpu_message_to_gpu_containers(table_view);
	std::vector<std::basic_string<char>> cpu_raw_buffers;
	for(int index = 0; index < buffer_sizes.size(); ++index) {
		std::basic_string<char> buffer;
		buffer.resize(buffer_sizes[index]);
		int currentDeviceId = 0; // TODO: CHECK device_id
		cudaSetDevice(currentDeviceId);
		cudaMemcpy((void *)buffer.data(), raw_buffers[index], buffer_sizes[index], cudaMemcpyHostToHost);
		cpu_raw_buffers.emplace_back(buffer);
	}
	return std::make_unique<ral::frame::BlazingHostTable>(column_offset, std::move(cpu_raw_buffers));
}

auto deserialize_from_gpu_raw_buffers(const std::vector<ColumnTransport> & columns_offsets,
									  const std::vector<rmm::device_buffer> & raw_buffers) {
	auto num_columns = columns_offsets.size();
	std::vector<std::unique_ptr<cudf::column>> received_samples(num_columns);
	std::vector<std::string> column_names(num_columns);

	assert(raw_buffers.size() >= 0);
	for(size_t i = 0; i < num_columns; ++i) {
		auto data_offset = columns_offsets[i].data;
		auto string_offset = columns_offsets[i].strings_data;
		if(string_offset != -1) {
			cudf::size_type num_strings = columns_offsets[i].metadata.size;
			std::unique_ptr<cudf::column> offsets_column
				= std::make_unique<cudf::column>(cudf::data_type{cudf::type_id::INT32}, num_strings + 1, std::move(raw_buffers[columns_offsets[i].strings_offsets]));

			cudf::size_type total_bytes = columns_offsets[i].strings_data_size;
			std::unique_ptr<cudf::column> chars_column	= std::make_unique<cudf::column>(cudf::data_type{cudf::type_id::INT8}, total_bytes, std::move(raw_buffers[columns_offsets[i].strings_data]));
			rmm::device_buffer null_mask;
			if (columns_offsets[i].strings_nullmask != -1)
				null_mask = rmm::device_buffer(std::move(raw_buffers[columns_offsets[i].strings_nullmask]));

			cudf::size_type null_count = columns_offsets[i].metadata.null_count;
			auto unique_column = cudf::make_strings_column(num_strings, std::move(offsets_column), std::move(chars_column), null_count, std::move(null_mask));
			received_samples[i] = std::move(unique_column);

		} else {
			cudf::data_type dtype = cudf::data_type{cudf::type_id(columns_offsets[i].metadata.dtype)};
			cudf::size_type column_size  =  (cudf::size_type)columns_offsets[i].metadata.size;

			cudf::valid_type * valid_ptr = nullptr;
			cudf::size_type valid_size = 0;
			if(columns_offsets[i].valid != -1) {
				// this is a valid
				auto valid_offset = columns_offsets[i].valid;
				auto unique_column = std::make_unique<cudf::column>(dtype, column_size, std::move(raw_buffers[data_offset]), std::move(raw_buffers[valid_offset]));
				received_samples[i] = std::move(unique_column);
			} else if (data_offset != -1){
				auto unique_column = std::make_unique<cudf::column>(dtype, column_size, std::move(raw_buffers[data_offset]));
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

std::shared_ptr<ReceivedMessage> deserialize_from_gpu(const MessageMetadata & message_metadata,
		const Address::MetaData & address_metadata,
		const std::vector<ColumnTransport> & columns_offsets,
		const std::vector<rmm::device_buffer> & raw_buffers) {

	auto node = Node(Address::TCP(address_metadata.ip, address_metadata.comunication_port, address_metadata.protocol_port), "");

	auto received_table = deserialize_from_gpu_raw_buffers(columns_offsets, raw_buffers);

    return std::make_shared<ReceivedDeviceMessage>(message_metadata.messageToken,
        message_metadata.contextToken,
        node,
        std::move(received_table),
        message_metadata.total_row_size);
}

//TODO: get column size_in_bytes
std::unique_ptr<ral::frame::BlazingTable> deserialize_from_cpu(const ral::frame::BlazingHostTable* host_table){
	std::vector<rmm::device_buffer> gpu_raw_buffers;
	const auto & raw_buffers = host_table->get_raw_buffers();
	for(int index = 0; index < raw_buffers.size(); ++index) {
		auto buffer_sz = raw_buffers[index].size();
		rmm::device_buffer dev_buffer(buffer_sz);
		int currentDeviceId = 0; // TODO: CHECK device_id
		cudaSetDevice(currentDeviceId);
		cudaMemcpy((void *)dev_buffer.data(),
			(const void *) raw_buffers[index].data(),
			buffer_sz,
			cudaMemcpyHostToDevice);
		gpu_raw_buffers.emplace_back(std::move(dev_buffer));
	}
	return deserialize_from_gpu_raw_buffers(host_table->get_columns_offsets(), gpu_raw_buffers);
}

}  // namespace messages
}  // namespace communication
}  // namespace ral
