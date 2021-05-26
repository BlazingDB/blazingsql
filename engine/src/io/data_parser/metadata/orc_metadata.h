#ifndef ORC_METADATA_H_
#define ORC_METADATA_H_
        
#include "common_metadata.h"
#include <cudf/io/orc_metadata.hpp>

void set_min_max(
	std::vector<std::vector<int64_t>> & minmax_metadata_table,
	cudf::io::column_statistics & statistic,
    int col_index);

std::unique_ptr<ral::frame::BlazingTable> get_minmax_metadata(
    std::vector<cudf::io::parsed_orc_statistics> & statistics,
    size_t total_stripes, int metadata_offset);

#endif	// ORC_METADATA_H_
