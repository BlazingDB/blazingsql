/*
 * CSVParser.cpp
 *
 *  Created on: Nov 29, 2018
 *      Author: felipe
 */

#include "CSVParser.h"
#include "Utils.cuh"
#include "cudf/legacy/io_types.hpp"
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

	// lets only read up to 48192 bytes. We are assuming that a full row will always be less than that
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
 

std::unique_ptr<ral::frame::BlazingTable> csv_parser::parse(
	std::shared_ptr<arrow::io::RandomAccessFile> file,
	const std::string & user_readable_file_handle,
	const Schema & schema,
	std::vector<size_t> column_indices) {

	if(file == nullptr) { 
		// return create_empty_table(schema.get_names(), schema.get_dtypes(), column_indices);  // do we need to create an empty table that has metadata?
		return nullptr;
	}

	cudf_io::read_csv_args new_csv_arg = this->csv_args;
	if(column_indices.size() > 0) {
		// copy column_indices into use_col_indexes (at the moment is ordered only)
		new_csv_arg.use_cols_indexes.resize(column_indices.size());
		new_csv_arg.use_cols_indexes.assign(column_indices.begin(), column_indices.end());

		cudf_io::table_with_metadata csv_table = read_csv_arg_arrow(new_csv_arg, file);

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
	std::vector<std::shared_ptr<arrow::io::RandomAccessFile>> files, ral::io::Schema & schema) {

	cudf_io::table_with_metadata table_out = read_csv_arg_arrow(csv_args, files[0], true);
	assert(table_out.tbl->num_columns() > 0);

	for(size_t i = 0; i < table_out.tbl->num_columns(); i++) {
		cudf::type_id type = table_out.tbl->get_column(i).type().id();
		size_t file_index = i;
		bool is_in_file = true;
		std::string name = table_out.metadata.column_names.at(i);
		schema.add_column(name, type, file_index, is_in_file);
	}
}

} /* namespace io */
} /* namespace ral */
