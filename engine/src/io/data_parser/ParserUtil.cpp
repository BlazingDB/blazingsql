#include "ParserUtil.h"

#include "utilities/StringUtils.h"
#include <Traits/RuntimeTraits.h>
#include <arrow/io/file.h>
#include <arrow/status.h>
#include <blazingdb/io/Library/Logging/Logger.h>

namespace ral {
namespace io {

// Used to create column new indexes that dont include columns that are already loaded
std::vector<size_t> get_column_indices_not_already_loaded(const std::vector<size_t> & column_indices_requested,
	const std::vector<std::string> & column_names,
	std::map<std::string, std::map<std::string, gdf_column_cpp>> & loaded_columns,
	const std::string & user_readable_file_handle) {
	std::vector<size_t> column_indices;
	for(auto column_index : column_indices_requested) {
		bool already_parsed_before = false;
		auto file_iter = loaded_columns.find(user_readable_file_handle);
		if(file_iter != loaded_columns.end()) {  // we have already parsed this file before
			auto col_iter = loaded_columns[user_readable_file_handle].find(column_names[column_index]);
			if(col_iter !=
				loaded_columns[user_readable_file_handle].end()) {  // we have already parsed this column before
				already_parsed_before = true;
			}
		}
		if(!already_parsed_before)
			column_indices.push_back(column_index);
	}
	return column_indices;
}


void get_columns_that_were_already_loaded(const std::vector<size_t> & column_indices_requested,
	const std::vector<std::string> & column_names,
	std::map<std::string, std::map<std::string, gdf_column_cpp>> & loaded_columns,
	const std::string & user_readable_file_handle,
	std::vector<gdf_column_cpp> & columns,
	std::vector<gdf_column_cpp> & columns_out) {
	int newly_parsed_col_idx = 0;
	for(auto column_index : column_indices_requested) {
		bool already_parsed_before = false;
		const std::string column_name = column_names[column_index];
		auto file_iter = loaded_columns.find(user_readable_file_handle);
		if(file_iter != loaded_columns.end()) {  // we have already parsed this file before
			auto col_iter = loaded_columns[user_readable_file_handle].find(column_name);
			if(col_iter !=
				loaded_columns[user_readable_file_handle].end()) {  // we have already parsed this column before
				already_parsed_before = true;
				columns_out.push_back(loaded_columns[user_readable_file_handle][column_name]);
			}
		}
		if(!already_parsed_before) {
			if(column_name != columns[newly_parsed_col_idx].name()) {
				Library::Logging::Logger().logError(ral::utilities::buildLogString(
					"", "", "", "ERROR: logic error when trying to use already loaded columns in ParquetParser"));
			}
			columns_out.push_back(columns[newly_parsed_col_idx]);
			loaded_columns[user_readable_file_handle][columns[newly_parsed_col_idx].name()] =
				columns[newly_parsed_col_idx];
			newly_parsed_col_idx++;
		}
	}
}


std::vector<gdf_column_cpp> create_empty_columns(const std::vector<std::string> & column_names,
	const std::vector<cudf::type_id> & column_types,
	const std::vector<gdf_time_unit> & column_time_units,
	const std::vector<size_t> & column_indices_requested) {
	std::vector<gdf_column_cpp> columns(column_indices_requested.size());

	for(size_t i = 0; i < column_indices_requested.size(); i++) {
		const size_t ind = column_indices_requested[i];

		cudf::type_id dtype = column_types[ind];
		gdf_time_unit time_unit = column_time_units[ind];
		const std::string & column_name = column_names[ind];

		columns[i].create_empty(dtype, column_name, time_unit);
	}

	return columns;
}

/**
 * reads contents of an arrow::io::RandomAccessFile in a char * buffer up to the number of bytes specified in
 * bytes_to_read for non local filesystems where latency and availability can be an issue it will retry until it has
 * exhausted its the read attemps and empty reads that are allowed
 */
gdf_error read_file_into_buffer(std::shared_ptr<arrow::io::RandomAccessFile> file,
	int64_t bytes_to_read,
	uint8_t * buffer,
	int total_read_attempts_allowed,
	int empty_reads_allowed) {
	if(bytes_to_read > 0) {
		int64_t total_read;
		arrow::Status status = file->Read(bytes_to_read, &total_read, buffer);

		if(!status.ok()) {
			return GDF_FILE_ERROR;
		}

		if(total_read < bytes_to_read) {
			// the following two variables shoudl be explained
			// Certain file systems can timeout like hdfs or nfs,
			// so we shoudl introduce the capacity to retry
			int total_read_attempts = 0;
			int empty_reads = 0;

			while(total_read < bytes_to_read && total_read_attempts < total_read_attempts_allowed &&
				  empty_reads < empty_reads_allowed) {
				int64_t bytes_read;
				status = file->Read(bytes_to_read - total_read, &bytes_read, buffer + total_read);
				if(!status.ok()) {
					return GDF_FILE_ERROR;
				}
				if(bytes_read == 0) {
					empty_reads++;
				}
				total_read += bytes_read;
			}
			if(total_read < bytes_to_read) {
				return GDF_FILE_ERROR;
			} else {
				return GDF_SUCCESS;
			}
		} else {
			return GDF_SUCCESS;
		}
	} else {
		return GDF_SUCCESS;
	}
}

} /* namespace io */
} /* namespace ral */
