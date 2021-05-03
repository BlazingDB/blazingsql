#include "BlazingHostTable.h"
#include "bmr/BlazingMemoryResource.h"
#include "bmr/BufferProvider.h"
#include "communication/CommunicationInterface/serializer.hpp"

using namespace fmt::literals;

namespace ral {
namespace frame {

BlazingHostTable::BlazingHostTable(const std::vector<ColumnTransport> &columns_offsets,
            std::vector<ral::memory::blazing_chunked_column_info> && chunked_column_infos,
            std::vector<std::unique_ptr<ral::memory::blazing_allocation_chunk>> && allocations)
        : columns_offsets{columns_offsets}, chunked_column_infos{std::move(chunked_column_infos)}, allocations{std::move(allocations)} {

    auto size = sizeInBytes();
    blazing_host_memory_resource::getInstance().allocate(size); // this only increments the memory usage counter for the host memory. This does not actually allocate

}

BlazingHostTable::~BlazingHostTable() {
    auto size = sizeInBytes();
    blazing_host_memory_resource::getInstance().deallocate(size); // this only decrements the memory usage counter for the host memory. This does not actually allocate
    for(auto i = 0; i < allocations.size(); i++){
        auto pool = allocations[i]->allocation->pool;
        pool->free_chunk(std::move(allocations[i]));
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

void BlazingHostTable::set_names(std::vector<std::string> names) {
    for(size_t i = 0; i < names.size(); i++){
        strcpy(columns_offsets[i].metadata.col_name, names[i].c_str());
    }
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
    std::vector<rmm::device_buffer> gpu_raw_buffers(chunked_column_infos.size());

    try{
        int buffer_index = 0;
        for(auto & chunked_column_info : chunked_column_infos){
            gpu_raw_buffers[buffer_index].resize(chunked_column_info.use_size);
            size_t position = 0;
            for(size_t i = 0; i < chunked_column_info.chunk_index.size(); i++){
                size_t chunk_index = chunked_column_info.chunk_index[i];
                size_t offset = chunked_column_info.offset[i];
                size_t chunk_size = chunked_column_info.size[i];
                cudaMemcpyAsync((void *) (gpu_raw_buffers[buffer_index].data() + position), allocations[chunk_index]->data + offset, chunk_size, cudaMemcpyHostToDevice,0);
                position += chunk_size;
            }
            buffer_index++;
        }
        cudaStreamSynchronize(0);
    }catch(std::exception & e){
        auto logger = spdlog::get("batch_logger");
        if (logger){
            logger->error("|||{info}|||||",
                    "info"_a="ERROR in BlazingHostTable::get_gpu_table(). What: {}"_format(e.what()));
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

const std::vector<ral::memory::blazing_chunked_column_info> & BlazingHostTable::get_blazing_chunked_column_infos() const {
    return this->chunked_column_infos;
}

}  // namespace frame
}  // namespace ral
