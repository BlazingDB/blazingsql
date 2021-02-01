#pragma once

#include <vector>
#include <string>
#include <memory>
#include "cudf/types.hpp"
#include "transport/ColumnTransport.h"
#include "bmr/BufferProvider.h"
#include "LogicPrimitives.h"

namespace ral {
namespace frame {

using ColumnTransport = blazingdb::transport::ColumnTransport;
class BlazingTable;  // forward declaration

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
        std::vector<std::unique_ptr<ral::memory::blazing_allocation_chunk>> && allocations);

    ~BlazingHostTable();

    std::vector<cudf::data_type> get_schema() const;

    std::vector<std::string> names() const;

    cudf::size_type num_rows() const ;

    cudf::size_type num_columns() const ;

    std::size_t sizeInBytes() ;

    void setPartitionId(const size_t &part_id) ;

    size_t get_part_id() ;

    const std::vector<ColumnTransport> & get_columns_offsets() const ;

    std::unique_ptr<BlazingTable> get_gpu_table() const;

    std::vector<ral::memory::blazing_allocation_chunk> get_raw_buffers() const;

    const std::vector<ral::memory::blazing_chunked_buffer> &  get_blazing_chunked_buffers() const;

private:
    std::vector<ColumnTransport> columns_offsets;
    std::vector<ral::memory::blazing_chunked_buffer> buffers;
    std::vector<std::unique_ptr<ral::memory::blazing_allocation_chunk>> allocations;

    
    size_t part_id;
};

}  // namespace frame
}  // namespace ral
