#include "BlazingHostTable.h"
#include "bmr/BlazingMemoryResource.h"
#include "bmr/BufferProvider.h"
#include "communication/CommunicationInterface/serializer.hpp"

using namespace fmt::literals;

namespace ral {
namespace frame {

BlazingHostTable::BlazingHostTable(const std::vector<ColumnTransport> &columns_offsets,
                                   std::vector<std::basic_string<char>> &&raw_buffers)
        : columns_offsets{columns_offsets}, raw_buffers{std::move(raw_buffers)} {
    auto size = sizeInBytes();
    blazing_host_memory_resource::getInstance().allocate(size);
}

BlazingHostTable::BlazingHostTable(const std::vector<ColumnTransport> &columns_offsets,
        std::vector<ral::memory::blazing_chunked_buffer> && buffers,
        std::vector<std::unique_ptr<ral::memory::blazing_allocation_chunk>> && allocations) {
            //TODO-WSM 
        }

BlazingHostTable::~BlazingHostTable() {
    auto size = sizeInBytes();
    blazing_host_memory_resource::getInstance().deallocate(size);
    for(auto & allocation : allocations){
        // TODO-WSM this free_chunk does not belong to pool?
        // allocation->allocation->pool->free_chunk(std::move(allocation));
    }
}

std::vector<cudf::data_type> BlazingHostTable::get_schema() const {
    std::vector<cudf::data_type> data_types(this->num_columns());
    std::transform(columns_offsets.begin(), columns_offsets.end(), data_types.begin(), [](auto &col) {
        int32_t dtype = col.metadata.dtype;
        return cudf::data_type{cudf::type_id(dtype)};
    });
    return data_types;
}

std::vector<std::string> BlazingHostTable::names() const {
    std::vector<std::string> col_names(this->num_columns());
    std::transform(columns_offsets.begin(), columns_offsets.end(), col_names.begin(),
                   [](auto &col) { return col.metadata.col_name; });
    return col_names;
}

cudf::size_type BlazingHostTable::num_rows() const {
    return columns_offsets.empty() ? 0 : columns_offsets.front().metadata.size;
}

cudf::size_type BlazingHostTable::num_columns() const {
    return columns_offsets.size();
}

std::size_t BlazingHostTable::sizeInBytes() {
    std::size_t total_size = 0L;
    for (auto &col : columns_offsets) {
        total_size += col.size_in_bytes;
    }
    return total_size;
}

void BlazingHostTable::setPartitionId(const size_t &part_id) {
    this->part_id = part_id;
}

size_t BlazingHostTable::get_part_id() {
    return this->part_id;
}

const std::vector<ColumnTransport> &BlazingHostTable::get_columns_offsets() const {
    return columns_offsets;
}



std::unique_ptr<BlazingTable> BlazingHostTable::get_gpu_table() const {

    std::vector<rmm::device_buffer> gpu_raw_buffers(buffers.size());
	
	try{
        
        int buffer_index = 0;
        for(auto & chunked_buffer : buffers){
            gpu_raw_buffers[buffer_index].resize(chunked_buffer.use_size);
            size_t position = 0;
            for(size_t i = 0; i < chunked_buffer.chunk_index.size(); i++){
                size_t chunk_index = chunked_buffer.chunk_index[i];
                size_t offset = chunked_buffer.offset[i];
                size_t chunk_size = chunked_buffer.size[i];
                //TODO-WSM allocations[chunk_index].data is not a thing
                // cudaMemcpyAsync((void *) (gpu_raw_buffers[buffer_index].data() + position), allocations[chunk_index].data + offset, chunk_size, cudaMemcpyHostToDevice,0);
                position += chunk_size;
            }
            buffer_index++;
        }
    	cudaStreamSynchronize(0);
	}catch(std::exception & e){
		auto logger = spdlog::get("batch_logger");
        if (logger){
            logger->error("|||{info}|||||",
                    "info"_a="ERROR in deserialize_from_cpu. What: {}"_format(e.what()));
        }
		throw;
	}
    return std::move(comm::deserialize_from_gpu_raw_buffers(columns_offsets,
									  gpu_raw_buffers));
}

std::vector<ral::memory::blazing_allocation_chunk> BlazingHostTable::get_raw_buffers() const {
    std::vector<ral::memory::blazing_allocation_chunk> chunks;
    for(auto & chunk : allocations){
        ral::memory::blazing_allocation_chunk new_chunk;
        new_chunk.size = chunk->size;
        new_chunk.data = chunk->data;
        new_chunk.allocation = nullptr;
        chunks.push_back(new_chunk);
    }

    return chunks;
}

}  // namespace frame
}  // namespace ral
