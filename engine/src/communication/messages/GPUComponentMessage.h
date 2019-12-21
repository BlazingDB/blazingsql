#pragma once

#include "GDFColumn.cuh"
#include <blazingdb/transport/Address.h>
#include <blazingdb/transport/ColumnTransport.h>
#include <blazingdb/transport/Message.h>
#include <blazingdb/transport/Node.h>

#include "Traits/RuntimeTraits.h"
#include <map>
#include <memory>
#include <mutex>
#include <numeric>
#include <tuple>

#include <cudf.h>
#include <nvstrings/NVCategory.h>
#include <nvstrings/NVStrings.h>

namespace ral {
namespace communication {
namespace messages {

using Node = blazingdb::transport::Node;
using Address = blazingdb::transport::Address;
using ColumnTransport = blazingdb::transport::ColumnTransport;
using GPUMessage = blazingdb::transport::GPUMessage;
using StrDataPtrTuple = std::tuple<char *, gdf_size_type, int *, gdf_size_type, unsigned char *, gdf_size_type>;

inline bool isGdfString(const gdf_column * column) {
	return GDF_STRING == column->dtype || GDF_STRING_CATEGORY == column->dtype;
}

inline gdf_size_type getValidCapacity(const gdf_column * column) {
	return column->null_count > 0 ? ral::traits::get_bitmask_size_in_bytes(column->size) : 0;
}

class GPUComponentMessage : public GPUMessage {
public:
	GPUComponentMessage(std::string const & messageToken,
		uint32_t contextToken,
		std::shared_ptr<Node> & sender_node,
		std::vector<std::pair<std::string, cudf::column>> & samples,
		std::uint64_t total_row_size = 0)
		: GPUMessage(messageToken, contextToken, sender_node), samples{samples} {
		this->metadata().total_row_size = total_row_size;
	}

	~GPUComponentMessage() {
		for(auto && e : strDataColToPtrMap) {
			char * stringsPointer;
			int * offsetsPointer;
			unsigned char * nullBitmask;
			std::tie(stringsPointer, std::ignore, offsetsPointer, std::ignore, nullBitmask, std::ignore) = e.second;
			RMM_TRY(RMM_FREE(stringsPointer, 0));
			RMM_TRY(RMM_FREE(offsetsPointer, 0));
			RMM_TRY(RMM_FREE(nullBitmask, 0));
		}
	}

	auto get_raw_pointers(gdf_column * column) {
		std::lock_guard<std::mutex> lock(strigDataMutex);

		auto strDataTupleIt = strDataColToPtrMap.find(column);
		if(strDataTupleIt != strDataColToPtrMap.end()) {
			return strDataTupleIt->second;
		}

		bool deleteCategory = false;
		void * category_ptr = column->dtype_info.category;
		if(!category_ptr) {
			category_ptr = NVCategory::create_from_array(nullptr, 0);
			deleteCategory = true;
		}
		auto size = column->size;
		auto null_count = column->null_count;
		std::string output;
		NVCategory * category = static_cast<NVCategory *>(category_ptr);
		NVStrings * nvStrings_ = category->gather_strings(category->values_cptr(), category->size(), true);
		gdf_size_type stringsLength_ = nvStrings_->size();
		auto offsetsLength_ = stringsLength_ + 1;

		int * const lengthPerStrings = new int[stringsLength_];
		nvStrings_->byte_count(lengthPerStrings, false);
		gdf_size_type stringsSize_ =
			std::accumulate(lengthPerStrings,
				lengthPerStrings + stringsLength_,
				0,
				[](int accumulator, int currentValue) { return accumulator + std::max(currentValue, 0); }) *
			sizeof(char);
		char * stringsPointer_ = nullptr;
		gdf_size_type offsetsSize_ = offsetsLength_ * sizeof(int);
		int * offsetsPointer_ = nullptr;
		RMM_TRY(RMM_ALLOC(reinterpret_cast<void **>(&stringsPointer_), stringsSize_, 0));
		RMM_TRY(RMM_ALLOC(reinterpret_cast<void **>(&offsetsPointer_), offsetsSize_, 0));

		gdf_size_type nullMaskSize_ = 0;
		unsigned char * nullBitmask_ = nullptr;
		if(null_count > 0) {
			nullMaskSize_ = ral::traits::get_bitmask_size_in_bytes(stringsLength_);
			RMM_TRY(RMM_ALLOC(reinterpret_cast<void **>(&nullBitmask_), nullMaskSize_, 0));
		}
		nvStrings_->create_offsets(stringsPointer_, offsetsPointer_, nullBitmask_, true);

		delete[] lengthPerStrings;
		NVStrings::destroy(nvStrings_);
		if(deleteCategory) {
			NVCategory::destroy(category);
		}

		auto strDataTuple =
			std::make_tuple(stringsPointer_, stringsSize_, offsetsPointer_, offsetsSize_, nullBitmask_, nullMaskSize_);
		strDataColToPtrMap[column] = strDataTuple;

		return strDataTuple;
	}

	static std::shared_ptr<GPUMessage> MakeFrom(const Message::MetaData & message_metadata,
		const Address::MetaData & address_metadata,
		const std::vector<ColumnTransport> & columns_offsets,
		std::vector<rmm::device_buffer> & raw_buffers) {  // gpu pointer
		auto node = std::make_shared<Node>(
			Address::TCP(address_metadata.ip, address_metadata.comunication_port, address_metadata.protocol_port));
		auto num_columns = columns_offsets.size();
		std::vector<std::pair<std::string, cudf::column_view>> received_samples(num_columns);
		assert(raw_buffers.size() >= 0);
		for(size_t i = 0; i < num_columns; ++i) {
			auto data_offset = columns_offsets[i].data;
			auto string_offset = columns_offsets[i].strings_data;
			if(string_offset != -1) {
				// TODO percy rommel cudf 0.12
//				// this is a string
//				// NVCategory *nvcategory_ptr = nullptr;
//				// auto string_offset = columns_offsets[i].strings_data;
//				// nvcategory_ptr = (NVCategory *) raw_buffers[string_offset];
//				char * stringsPointer = (char *) raw_buffers[columns_offsets[i].strings_data];
//				int * offsetsPointer = (int *) raw_buffers[columns_offsets[i].strings_offsets];
//				unsigned char * nullMaskPointer = nullptr;
//				if(columns_offsets[i].strings_nullmask != -1) {
//					nullMaskPointer = (unsigned char *) raw_buffers[columns_offsets[i].strings_nullmask];
//				}

//				auto nvcategory_ptr = NVCategory::create_from_offsets(reinterpret_cast<const char *>(stringsPointer),
//					columns_offsets[i].metadata.size,
//					reinterpret_cast<const int *>(offsetsPointer),
//					reinterpret_cast<const unsigned char *>(nullMaskPointer),
//					columns_offsets[i].metadata.null_count,
//					true);
				
//				// TODO rommel percy cudf0.12
//				//received_samples[i].create_gdf_column(nvcategory_ptr, columns_offsets[i].metadata.size, (char *) columns_offsets[i].metadata.col_name);

//				RMM_TRY(RMM_FREE(stringsPointer, 0));
//				RMM_TRY(RMM_FREE(offsetsPointer, 0));
//				RMM_TRY(RMM_FREE(nullMaskPointer, 0));
			} else {
				rmm::device_buffer valid_ptr;
				if(columns_offsets[i].valid != -1) {
					// this is a valid
					int valid_offset = columns_offsets[i].valid;
					valid_ptr = raw_buffers[valid_offset];
				}
				
				cudf::data_type dtype = columns_offsets[i].metadata.dtype;
				cudf::size_type size = columns_offsets[i].metadata.size;
				rmm::device_buffer data = raw_buffers[data_offset];
			    rmm::device_buffer null_mask = valid_ptr;
				cudf::size_type null_count = columns_offsets[i].metadata.null_count;
				std::string col_name(columns_offsets[i].metadata.col_name);
				cudf::column column(dtype, size, data, null_mask, null_count);
				received_samples[i] = std::make_pair(col_name, column);
			}
			received_samples[i].first = std::string{columns_offsets[i].metadata.col_name};
		}
		return std::make_shared<GPUComponentMessage>(message_metadata.messageToken,
			message_metadata.contextToken,
			node,
			received_samples,
			message_metadata.total_row_size);
	}
	std::vector<std::pair<std::string, cudf::column>> getSamples() { return samples; }

protected:
	std::vector<std::pair<std::string, cudf::column>> samples;

	std::map<gdf_column *, StrDataPtrTuple> strDataColToPtrMap;

	std::mutex strigDataMutex;
};

}  // namespace messages
}  // namespace communication
}  // namespace ral
