/*
 * CSVParser.cpp
 *
 *  Created on: Nov 29, 2018
 *      Author: felipe
 */

#include "CSVParser.h"
#include <arrow/buffer.h>
#include <arrow/io/memory.h>
#include <numeric>
#include <sys/types.h>

#include <blazingdb/io/Library/Logging/Logger.h>
#include "ArgsUtil.h"

#define checkError(error, txt)                                                                                         \
	if(error != GDF_SUCCESS) {                                                                                         \
		std::cerr << "ERROR:  " << error << "  in " << txt << std::endl;                                               \
		return error;                                                                                                  \
	}

namespace ral {
namespace io {

csv_parser::csv_parser(std::map<std::string, std::string> args_map_) : args_map{args_map_} {}

csv_parser::~csv_parser() {}

std::unique_ptr<ral::frame::BlazingTable> csv_parser::parse_batch(
	ral::io::data_handle handle,
	const Schema & schema,
	std::vector<int> column_indices,
	std::vector<cudf::size_type> row_groups) {
	std::shared_ptr<arrow::io::RandomAccessFile> file = handle.file_handle;

	if(file == nullptr) {
		return schema.makeEmptyBlazingTable(column_indices);
	}

	if(column_indices.size() > 0) {

		// copy column_indices into use_col_indexes (at the moment is ordered only)
		auto arrow_source = cudf::io::arrow_io_source{file};
		cudf::io::csv_reader_options args = getCsvReaderOptions(args_map, arrow_source);
		args.set_use_cols_indexes(column_indices);

		if (args.get_header() > 0) {
			args.set_header(args.get_header());
		} else if (args_map["has_header_csv"] == "True") {
			args.set_header(0);
		} else {
			args.set_header(-1);
		}

		// Overrride `_byte_range_size` param to read first `max_bytes_chunk_read` bytes (note: always reads complete rows)
		auto iter = args_map.find("max_bytes_chunk_read");
		if(iter != args_map.end()) {
			auto chunk_size = std::stoll(iter->second);
			args.set_byte_range_offset(chunk_size * row_groups[0]);
			args.set_byte_range_size(chunk_size);
		}

		cudf::io::table_with_metadata csv_table = cudf::io::read_csv(args);

		if(csv_table.tbl->num_columns() <= 0)
			Library::Logging::Logger().logWarn("csv_parser::parse no columns were read");

		// column_indices may be requested in a specific order (not necessarily sorted), but read_csv will output the
		// columns in the sorted order, so we need to put them back into the order we want
		std::vector<size_t> idx(column_indices.size());
		std::iota(idx.begin(), idx.end(), 0);
		// sort indexes based on comparing values in column_indices
		std::sort(idx.begin(), idx.end(), [&column_indices](size_t i1, size_t i2) {
			return column_indices[i1] < column_indices[i2];
		});

		std::vector< std::unique_ptr<cudf::column> > columns_out;
		std::vector<std::string> column_names_out;

		columns_out.resize(column_indices.size());
		column_names_out.resize(column_indices.size());

		std::vector< std::unique_ptr<cudf::column> > table = csv_table.tbl->release();

		for(size_t i = 0; i < column_indices.size(); i++) {
			columns_out[idx[i]] = std::move(table[i]);
			column_names_out[idx[i]] = csv_table.metadata.column_names[i];
		}

		std::unique_ptr<CudfTable> cudf_tb = std::make_unique<CudfTable>(std::move(columns_out));
		return std::make_unique<ral::frame::BlazingTable>(std::move(cudf_tb), column_names_out);
	}
	return nullptr;
}


void csv_parser::parse_schema(
	ral::io::data_handle handle, ral::io::Schema & schema) {

  auto file = handle.file_handle;
	auto arrow_source = cudf::io::arrow_io_source{file};
	cudf::io::csv_reader_options args = getCsvReaderOptions(args_map, arrow_source);

	// if names were not passed when create_table
	if (args.get_header() == 0) {
		schema.set_has_header_csv(true);
	}

	int64_t num_bytes = file->GetSize().ValueOrDie();

	// lets only read up to 48192 bytes. We are assuming that a full row will always be less than that
	if(num_bytes > 48192) {
		args.set_nrows(1);
		args.set_skipfooter(0);
	}
	cudf::io::table_with_metadata table_out = cudf::io::read_csv(args);
	file->Close();

	for(int i = 0; i < table_out.tbl->num_columns(); i++) {
		cudf::type_id type = table_out.tbl->get_column(i).type().id();
		size_t file_index = i;
		bool is_in_file = true;
		std::string name = table_out.metadata.column_names.at(i);
		schema.add_column(name, type, file_index, is_in_file);
	}
}

size_t csv_parser::max_bytes_chunk_size() const {
	auto iter = args_map.find("max_bytes_chunk_read");
	if(iter == args_map.end()) {
		return 0;
	}

	return std::stoll(iter->second);
}

int64_t GetFileSize(std::string filename)
{
    struct stat stat_buf;
    int rc = stat(filename.c_str(), &stat_buf);
    return rc == 0 ? (int64_t)stat_buf.st_size : -1;
}

std::unique_ptr<ral::frame::BlazingTable> csv_parser::get_metadata(
	std::vector<ral::io::data_handle> handles, int offset,
	std::map<std::string, std::string> args_map)
{
	int64_t size_max_batch;
	if (args_map.find("max_bytes_chunk_read") != args_map.end()) {
		// At this level `size_max_batch` should be diff to 0
		size_max_batch = (int64_t)to_int(args_map.at("max_bytes_chunk_read"));
	}

	std::vector<int64_t> num_total_bytes_per_file(handles.size());
	std::vector<size_t> num_batches_per_file(handles.size());
	for (size_t i = 0; i < handles.size(); ++i) {
		num_total_bytes_per_file[i] = GetFileSize(handles[i].uri.toString(true));
		num_batches_per_file[i] = std::ceil((double) num_total_bytes_per_file[i] / size_max_batch);
	}
	
	std::vector<int> file_index_values, row_group_values;
	for (int i = 0; i < num_batches_per_file.size(); ++i) {
		for (int j = 0; j < num_batches_per_file[i]; ++j) {
			file_index_values.push_back(i + offset);
			row_group_values.push_back(j);
		}
	}

	std::vector< std::unique_ptr<cudf::column> > columns;
	columns.emplace_back( ral::utilities::vector_to_column(file_index_values, cudf::data_type(cudf::type_id::INT32)) );
	columns.emplace_back( ral::utilities::vector_to_column(row_group_values, cudf::data_type(cudf::type_id::INT32)) );

	std::vector<std::string> metadata_names = {"file_handle_index", "row_group_index"};
	auto metadata_table = std::make_unique<cudf::table>(std::move(columns));

	return std::make_unique<ral::frame::BlazingTable>(std::move(metadata_table), metadata_names);
}

} /* namespace io */
} /* namespace ral */
