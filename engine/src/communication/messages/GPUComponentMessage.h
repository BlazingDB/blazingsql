#pragma once

#include <blazingdb/transport/Address.h>
#include <blazingdb/transport/ColumnTransport.h>
#include <blazingdb/transport/Message.h>
#include <blazingdb/transport/Node.h>

#include <map>
#include <memory>
#include <mutex>
#include <numeric>
#include <tuple>

#include <cudf/copying.hpp>
#include <cudf/column/column.hpp>
#include <cudf/column/column_view.hpp>
#include <cudf/null_mask.hpp>
#include <cudf/column/column_factories.hpp>
#include <cudf/types.hpp>
#include <execution_graph/logic_controllers/LogicPrimitives.h>
#include "Traits/RuntimeTraits.h"

#include "GDFColumn.cuh"

#include "from_cudf/cpp_tests/utilities/column_utilities.hpp"


namespace ral {
namespace communication {
namespace messages {

namespace experimental {

using Node = blazingdb::transport::experimental::Node;
using Address = blazingdb::transport::experimental::Address;
using ColumnTransport = blazingdb::transport::experimental::ColumnTransport;
using GPUMessage = blazingdb::transport::experimental::GPUMessage;
using GPUReceivedMessage = blazingdb::transport::experimental::GPUReceivedMessage;

inline bool isGdfString(const cudf::column_view& column) {
	cudf::type_id col_type = column.type().id();
	bool ret = (cudf::type_id::STRING == col_type || cudf::type_id::CATEGORY == col_type);
	return ret;
}


class GPUComponentReceivedMessage : public GPUReceivedMessage {
public:

	GPUComponentReceivedMessage(std::string const & messageToken,
						uint32_t contextToken,
						Node  & sender_node,
					    std::unique_ptr<ral::frame::BlazingTable> && samples,
						std::uint64_t total_row_size = 0)
		: GPUReceivedMessage(messageToken, contextToken, sender_node),
		  table(std::move(samples)) {
		this->metadata().total_row_size = total_row_size;
	} 
	
	std::unique_ptr<ral::frame::BlazingTable>  releaseBlazingTable() { return std::move(table); }

	int32_t getTotalRowSize() { return this->metadata().total_row_size; };

protected:
	std::unique_ptr<ral::frame::BlazingTable> table;
};

class GPUComponentMessage : public GPUMessage {
public:
	GPUComponentMessage(std::string const & messageToken,
		uint32_t contextToken,
		Node  & sender_node,
		const ral::frame::BlazingTableView & samples,
		std::uint64_t total_row_size = 0)
		: GPUMessage(messageToken, contextToken, sender_node), table_view{samples} {
		this->metadata().total_row_size = total_row_size;
	}

	virtual raw_buffer GetRawColumns() override {
		std::vector<int> buffer_sizes;
		std::vector<const char *> raw_buffers;
		std::vector<ColumnTransport> column_offset;
		std::vector<std::unique_ptr<rmm::device_buffer>> temp_bitmasks_scope_holder;
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
				.strings_nullmask = -1};
			strcpy(col_transport.metadata.col_name, table_view.names().at(i).c_str());
			if(isGdfString(column)) {
				auto num_children = column.num_children();
				assert(num_children == 2);
				auto offsets_column = column.child(0);
				auto chars_column = column.child(1);

			 	col_transport.strings_data = raw_buffers.size();
				buffer_sizes.push_back(chars_column.size());
				raw_buffers.push_back(chars_column.data<char>());
				col_transport.strings_data_size = chars_column.size();

				cudf::data_type offset_dtype (cudf::type_id::INT32);
				col_transport.strings_offsets = raw_buffers.size();
				col_transport.strings_offsets_size = offsets_column.size() * cudf::size_of(offset_dtype);
				buffer_sizes.push_back(col_transport.strings_offsets_size);
				raw_buffers.push_back(offsets_column.data<char>());
				
				if(column.has_nulls()) {
					col_transport.strings_nullmask = raw_buffers.size();
					buffer_sizes.push_back(cudf::bitmask_allocation_size_bytes(column.size()));
					if (column.offset() == 0){
						raw_buffers.push_back((const char *)column.null_mask());
					} else {
						temp_bitmasks_scope_holder.emplace_back(std::make_unique<rmm::device_buffer>(
							cudf::copy_bitmask(column.null_mask(), column.offset(), column.offset() + column.size())));
						raw_buffers.push_back((const char *)temp_bitmasks_scope_holder.back()->data());					
					}
				}
			} else if(column.has_nulls() and column.null_count() > 0) {
				// case: valid
				col_transport.data = raw_buffers.size();
				buffer_sizes.push_back(column.size() * cudf::size_of(column.type()));
				raw_buffers.push_back(column.head<char>() + column.offset() * cudf::size_of(column.type())); // here we are getting the beginning of the buffer and manually calculating the offset.
				col_transport.valid = raw_buffers.size();
				buffer_sizes.push_back(cudf::bitmask_allocation_size_bytes(column.size()));
				if (column.offset() == 0){
					raw_buffers.push_back((const char *)column.null_mask());
				} else {
					temp_bitmasks_scope_holder.emplace_back(std::make_unique<rmm::device_buffer>(
						cudf::copy_bitmask(column.null_mask(), column.offset(), column.offset() + column.size())));
					raw_buffers.push_back((const char *)temp_bitmasks_scope_holder.back()->data());					
				}
			} else {
				// case: data
				col_transport.data = raw_buffers.size();
				buffer_sizes.push_back(column.size() * cudf::size_of(column.type()));
				raw_buffers.push_back(column.head<char>() + column.offset() * cudf::size_of(column.type())); // here we are getting the beginning of the buffer and manually calculating the offset.
			}
			column_offset.push_back(col_transport);
		}
		return std::make_tuple(buffer_sizes, raw_buffers, column_offset, std::move(temp_bitmasks_scope_holder));
	}

	static std::shared_ptr<GPUReceivedMessage> MakeFrom(const Message::MetaData & message_metadata,
		const Address::MetaData & address_metadata,
		const std::vector<ColumnTransport> & columns_offsets,
		const std::vector<rmm::device_buffer> & raw_buffers) {  // gpu pointer
		auto node = Node(Address::TCP(address_metadata.ip, address_metadata.comunication_port, address_metadata.protocol_port));
		auto num_columns = columns_offsets.size();
		std::vector<std::unique_ptr<cudf::column>> received_samples(num_columns);
		std::vector<std::string> column_names(num_columns);

		assert(raw_buffers.size() >= 0);
		for(size_t i = 0; i < num_columns; ++i) {
			auto column = new cudf::column();
			auto data_offset = columns_offsets[i].data;
			auto string_offset = columns_offsets[i].strings_data;
			if(string_offset != -1) { 
				cudf::size_type num_strings = columns_offsets[i].metadata.size;
				std::unique_ptr<cudf::column> offsets_column 
					= std::make_unique<cudf::column>(cudf::data_type{cudf::INT32}, num_strings + 1, std::move(raw_buffers[columns_offsets[i].strings_offsets]));
				
				cudf::size_type total_bytes = columns_offsets[i].strings_data_size;
				std::unique_ptr<cudf::column> chars_column	= std::make_unique<cudf::column>(cudf::data_type{cudf::INT8}, total_bytes, std::move(raw_buffers[columns_offsets[i].strings_data]));
				rmm::device_buffer null_mask(std::move(raw_buffers[columns_offsets[i].strings_nullmask])); 
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
				} else {
					auto unique_column = std::make_unique<cudf::column>(dtype, column_size, std::move(raw_buffers[data_offset]));
					received_samples[i] = std::move(unique_column);
				}
			}
			column_names[i] = std::string{columns_offsets[i].metadata.col_name};
		}
		auto unique_table = std::make_unique<cudf::experimental::table>(std::move(received_samples));
		auto received_table = std::make_unique<ral::frame::BlazingTable>(std::move(unique_table), column_names);
		return std::make_shared<GPUComponentReceivedMessage>(message_metadata.messageToken,
			message_metadata.contextToken,
			node,
			std::move(received_table),
			message_metadata.total_row_size);
	}

	ral::frame::BlazingTableView getTableView() { return table_view; }

protected:
	ral::frame::BlazingTableView table_view; 
};


}  // namespace experimental
}  // namespace messages
}  // namespace communication
}  // namespace ral

