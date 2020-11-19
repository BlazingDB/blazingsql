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
    BlazingHostTable(const std::vector<ColumnTransport> &columns_offsets, std::vector<std::basic_string<char>> &&raw_buffers);

    ~BlazingHostTable();

    std::vector<cudf::data_type> get_schema() const;

    std::vector<std::string> names() const;

    cudf::size_type num_rows() const ;

    cudf::size_type num_columns() const ;

    std::size_t sizeInBytes() ;

    void setPartitionId(const size_t &part_id) ;

    size_t get_part_id() ;

    const std::vector<ColumnTransport> & get_columns_offsets() const ;

    const std::vector<std::basic_string<char>> & get_raw_buffers() const ;

private:
    std::vector<ColumnTransport> columns_offsets;
    std::vector<std::basic_string<char>> raw_buffers;
    size_t part_id;
};

}  // namespace frame
}  // namespace ral
