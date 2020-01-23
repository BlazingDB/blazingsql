/*
 * CSVParser.cpp
 *
 *  Created on: Nov 29, 2018
 *      Author: felipe
 */

#include "CSVParser.h"
#include "../Utils.cuh"
#include "cudf/legacy/io_types.hpp"
#include "io/data_parser/ParserUtil.h"
#include <arrow/buffer.h>
#include <arrow/io/interfaces.h>
#include <arrow/io/memory.h>
#include <iostream>
#include <numeric>

#include <cudf/table/table.hpp>
#include <blazingdb/io/Library/Logging/Logger.h>
#include <cudf/types.hpp>

#include <algorithm>

#define checkError(error, txt)                                                                                         \
	if(error != GDF_SUCCESS) {                                                                                         \
		std::cerr << "ERROR:  " << error << "  in " << txt << std::endl;                                               \
		return error;                                                                                                  \
	}

namespace ral {
namespace io {

csv_parser::csv_parser(cudf_io::read_csv_args args_) : csv_args{args_} {}

csv_parser::~csv_parser() {}

cudf_io::table_with_metadata read_csv_arg_arrow(cudf_io::read_csv_args new_csv_args,
	std::shared_ptr<arrow::io::RandomAccessFile> arrow_file_handle,
	bool first_row_only = false) {
	
	int64_t num_bytes;
	arrow_file_handle->GetSize(&num_bytes);

	// lets only read up to 8192 bytes. We are assuming that a full row will always be less than that
	if(first_row_only && num_bytes > 48192) {
		new_csv_args.byte_range_size = 48192;
		new_csv_args.nrows = 1;
		new_csv_args.skipfooter = 0;
	}

	new_csv_args.source = cudf_io::source_info(arrow_file_handle);

	if(new_csv_args.nrows != -1)
		new_csv_args.skipfooter = 0;

	cudf_io::table_with_metadata table_out = cudf_io::read_csv(new_csv_args);

	arrow_file_handle->Close();

	return std::move(table_out);
}
 

std::unique_ptr<cudf::column> make_empty_column(cudf::data_type type) {
  return std::make_unique<cudf::column>(type, 0, rmm::device_buffer{});
}

std::unique_ptr<ral::frame::BlazingTable> create_empty_table(
	const std::vector<std::string> &column_names, 
	const std::vector<cudf::type_id> &dtypes, 
	const std::vector<size_t> &column_indices) {
	std::vector<std::unique_ptr<cudf::column>> columns(column_indices.size());

	for (auto idx : column_indices) {
		columns[idx] =  make_empty_column(cudf::data_type(dtypes[idx]));
	}
	auto table = std::make_unique<cudf::experimental::table>(std::move(columns));
	return std::make_unique<ral::frame::BlazingTable>(std::move(table), column_names);
}

ral::frame::TableViewPair csv_parser::parse(
	std::shared_ptr<arrow::io::RandomAccessFile> file,
	const std::string & user_readable_file_handle,
	const Schema & schema,
	std::vector<size_t> column_indices) {

	// including all columns by default
	if(column_indices.size() == 0) {
		column_indices.resize(schema.get_num_columns());
		std::iota(column_indices.begin(), column_indices.end(), 0);
	}

	if(file == nullptr) { 
		// return create_empty_table(schema.get_names(), schema.get_dtypes(), column_indices);  // do we need to create an empty table that has metadata?
		return std::make_pair(nullptr, ral::frame::BlazingTableView());
	}

	cudf_io::read_csv_args csv_arg = this->csv_args;
	if(column_indices.size() > 0) {
		// copy column_indices into use_col_indexes (at the moment is ordered only)
		csv_args.use_cols_indexes.resize(column_indices.size());
		csv_args.use_cols_indexes.assign(column_indices.begin(), column_indices.end());

		cudf_io::table_with_metadata csv_table = read_csv_arg_arrow(csv_arg, file);

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

		std::unique_ptr<ral::frame::BlazingTable> table_out = std::make_unique<ral::frame::BlazingTable>(std::move(csv_table.tbl), csv_table.metadata.column_names);
		ral::frame::BlazingTableView table_out_view = table_out->toBlazingTableView();
		return std::make_pair(std::move(table_out), table_out_view);
	}
	// return create_empty_table(schema.get_names(), schema.get_dtypes(), column_indices);  // do we need to create an empty table that has metadata?
	return std::make_pair(nullptr, ral::frame::BlazingTableView());
}
	
void csv_parser::parse_schema(
	std::vector<std::shared_ptr<arrow::io::RandomAccessFile>> files, ral::io::Schema & schema) {

	cudf_io::table_with_metadata table_out = read_csv_arg_arrow(csv_args, files[0], true);
	assert(table_out.tbl->num_columns() > 0);

	for(size_t i = 0; i < table_out.tbl->num_columns(); i++) {
		std::string name = "";
		if (i < csv_args.names.size()) 
			name = csv_args.names[i];
		cudf::type_id type = table_out.tbl->get_column(i).type().id();
		size_t file_index = i;
		bool is_in_file = true;
		schema.add_column(name, type, file_index, is_in_file);
	}
}

} /* namespace io */
} /* namespace ral */
