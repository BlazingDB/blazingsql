#pragma once
#include <vector>
#include <string>
#include <blazingdb/transport/ColumnTransport.h>

#include "cudf/column/column_view.hpp"
#include "cudf/table/table.hpp"
#include "cudf/table/table_view.hpp"
#include <cudf/io/functions.hpp>
#include <bmr/BlazingMemoryResource.h>

namespace ral {
namespace frame {
using ColumnTransport = blazingdb::transport::experimental::ColumnTransport;

class BlazingHostTable {
public:
    BlazingHostTable(const std::vector<ColumnTransport> &columns_offsets, std::vector<std::basic_string<char>> &&raw_buffers)
            : columns_offsets{columns_offsets}, raw_buffers{std::move(raw_buffers)}
    {
        auto size = sizeInBytes();
        blazing_host_memory_mesource::getInstance().allocate(size);
    }
    ~BlazingHostTable() {
        auto size = sizeInBytes();
        blazing_host_memory_mesource::getInstance().deallocate(size);
    }

    std::vector<cudf::data_type> get_schema() const {
        std::vector<cudf::data_type> data_types(this->num_columns());
        std::transform(columns_offsets.begin(), columns_offsets.end(), data_types.begin(), [](auto & col){
            int32_t dtype = col.metadata.dtype;
            return cudf::data_type{cudf::type_id(dtype)};
        });
        return data_types;
    }

    std::vector<std::string> names() const {
        std::vector<std::string> col_names(this->num_columns());
        std::transform(columns_offsets.begin(), columns_offsets.end(), col_names.begin(), [](auto & col){ return col.metadata.col_name; });
        return col_names;
    }

    cudf::size_type num_rows() const {
        return columns_offsets.empty() ? 0 : columns_offsets.front().metadata.size;
    }

    cudf::size_type num_columns() const {
        return columns_offsets.size();
    }

    unsigned long long sizeInBytes() {
        unsigned long long total_size = 0L;
        for (auto &col : columns_offsets) {
            total_size += col.size_in_bytes;
        }
        return total_size;
    }

    void setPartitionId(const size_t &part_id) {
        this->part_id = part_id;
    }

    size_t get_part_id() {
        return this->part_id;
    }

    const std::vector<ColumnTransport> & get_columns_offsets() const {
        return columns_offsets;
    }

    const std::vector<std::basic_string<char>> & get_raw_buffers() const{
        return raw_buffers;
    }

private:
    std::vector<ColumnTransport> columns_offsets;
    std::vector<std::basic_string<char>> raw_buffers;
    size_t part_id;
};

}  // namespace frame
}  // namespace ral
