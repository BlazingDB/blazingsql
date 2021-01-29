#pragma once

#include <vector>
#include <string>
#include "cudf/types.hpp"
#include "transport/ColumnTransport.h"

namespace ral {
namespace frame {

using ColumnTransport = blazingdb::transport::ColumnTransport;

/**
	@brief A class that represents the BlazingTable store in host memory.
    This implementation uses only raw buffers and offtets that represent a BlazingTable.
    The reference to implement this class was based on the way how BlazingTable objects are send/received 
    by the communication library.
*/ 
class BlazingHostTable {
public:
    BlazingHostTable(const std::vector<ColumnTransport> &columns_offsets,
    std::vector<ral::memory::blazing_chunked_buffer> && buffers,
    std::vector<std::unique_ptr<ral::memory::blazing_allocation_chunk> && allocations);

    ~BlazingHostTable();

    std::vector<cudf::data_type> get_schema() const;

    std::vector<std::string> names() const;

    cudf::size_type num_rows() const ;

    cudf::size_type num_columns() const ;

    std::size_t sizeInBytes() ;

    void setPartitionId(const size_t &part_id) ;

    size_t get_part_id() ;

    const std::vector<ColumnTransport> & get_columns_offsets() const ;

    std::vector<rmm::device_buffer> & get_gpu_table() const;

    std::vector<ral::memory::blazing_allocation_chunk> BlazingHostTable::get_raw_buffers();

private:
    std::vector<ColumnTransport> columns_offsets;
    std::vector<ral::memory::blazing_chunked_buffer> buffers;
    std::vector<std::unique_ptr<ral::memory::blazing_allocation_chunk> allocations;

    
    size_t part_id;
};

}  // namespace frame
}  // namespace ral
