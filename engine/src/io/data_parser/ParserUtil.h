#ifndef PARSERUTIL_H_
#define PARSERUTIL_H_

#include <vector>
#include <map>
#include <string>

#include "GDFColumn.cuh"

namespace ral {
namespace io {

    std::vector<size_t> get_column_indices_not_already_loaded(
            const std::vector<size_t> & column_indices_requested, 
            const std::vector<std::string> & column_names,
            std::map<std::string,std::map<std::string, gdf_column_cpp>> & loaded_columns,
            const std::string & user_readable_file_handle);

    void get_columns_that_were_already_loaded(
            const std::vector<size_t> & column_indices_requested, 
            const std::vector<std::string> & column_names,
            std::map<std::string,std::map<std::string, gdf_column_cpp>> & loaded_columns,
            const std::string & user_readable_file_handle,
            std::vector<gdf_column_cpp> & columns,
            std::vector<gdf_column_cpp> & columns_out);

    std::vector<gdf_column_cpp> create_empty_columns(const std::vector<std::string> & column_names,
            const std::vector<gdf_dtype> & column_types,
            const std::vector<gdf_time_unit> & column_time_units,
            const std::vector<size_t> & column_indices_requested);

    gdf_error read_file_into_buffer(std::shared_ptr<arrow::io::RandomAccessFile> file, 
            int64_t bytes_to_read, uint8_t* buffer, int total_read_attempts_allowed, int empty_reads_allowed);

} /* namespace io */
} /* namespace ral */

#endif /* PARSERUTIL_H_ */