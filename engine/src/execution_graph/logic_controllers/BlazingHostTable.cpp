#include "BlazingHostTable.h"
#include "bmr/BlazingMemoryResource.h"

namespace ral {
namespace frame {

BlazingHostTable::BlazingHostTable(const std::vector<ColumnTransport> &columns_offsets,
                                   std::vector<std::basic_string<char>> &&raw_buffers)
        : columns_offsets{columns_offsets}, raw_buffers{std::move(raw_buffers)} {
    auto size = sizeInBytes();
    blazing_host_memory_resource::getInstance().allocate(size);
}

BlazingHostTable::~BlazingHostTable() {
    auto size = sizeInBytes();
    blazing_host_memory_resource::getInstance().deallocate(size);
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

const std::vector<std::basic_string<char>> &BlazingHostTable::get_raw_buffers() const {
    return raw_buffers;
}

}  // namespace frame
}  // namespace ral
