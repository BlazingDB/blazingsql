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

#include <algorithm>
#include <numeric>
#define checkError(error, txt)                                                                                         \
	if(error != GDF_SUCCESS) {                                                                                         \
		std::cerr << "ERROR:  " << error << "  in " << txt << std::endl;                                               \
		return error;                                                                                                  \
	}

namespace ral {
namespace io {


cudf::table read_csv_arg_arrow(cudf::csv_read_arg args,
	std::shared_ptr<arrow::io::RandomAccessFile> arrow_file_handle,
	bool first_row_only = false) {
	int64_t num_bytes;
	arrow_file_handle->GetSize(&num_bytes);

	// lets only read up to 8192 bytes. We are assuming that a full row will always be less than that
	if(first_row_only && num_bytes > 48192) {
		args.byte_range_size = 48192;
		args.nrows = 1;
		args.skipfooter = 0;
	}

	args.source = cudf::source_info(arrow_file_handle);

	if(args.nrows != -1)
		args.skipfooter = 0;

	cudf::table table_out = read_csv(args);

	arrow_file_handle->Close();

	return table_out;
}


csv_parser::csv_parser(cudf::csv_read_arg arg) : csv_arg{arg} {}

csv_parser::~csv_parser() {}

// schema is not really necessary yet here, but we want it to maintain compatibility
void csv_parser::parse(std::shared_ptr<arrow::io::RandomAccessFile> file,
	const std::string & user_readable_file_handle,
	std::vector<gdf_column_cpp> & columns_out,
	const Schema & schema,
	std::vector<size_t> column_indices) {
	// including all columns by default
	if(column_indices.size() == 0) {
		column_indices.resize(schema.get_num_columns());
		std::iota(column_indices.begin(), column_indices.end(), 0);
	}

	if(file == nullptr) {
		columns_out =
			create_empty_columns(schema.get_names(), schema.get_dtypes(), column_indices);
		return;
	}
	auto csv_arg = this->csv_arg;
	if(column_indices.size() > 0) {
		// copy column_indices into use_col_indexes (at the moment is ordered only)
		csv_arg.use_cols_indexes.resize(column_indices.size());
		csv_arg.use_cols_indexes.assign(column_indices.begin(), column_indices.end());

		// TODO percy cudf0.12 port cudf::column and io stuff
//		cudf::table table_out = read_csv_arg_arrow(csv_arg, file);
//		assert(table_out.num_columns() > 0);

		// column_indices may be requested in a specific order (not necessarily sorted), but read_csv will output the
		// columns in the sorted order, so we need to put them back into the order we want
		std::vector<size_t> idx(column_indices.size());
		std::iota(idx.begin(), idx.end(), 0);
		// sort indexes based on comparing values in column_indices
		std::sort(idx.begin(), idx.end(), [&column_indices](size_t i1, size_t i2) {
			return column_indices[i1] < column_indices[i2];
		});

		columns_out.resize(column_indices.size());

		// TODO percy cudf0.12 port cudf::column and io stuff
//		for(size_t i = 0; i < columns_out.size(); i++) {
//			if(table_out.get_column(i)->dtype == GDF_STRING) {
//				NVStrings * strs = static_cast<NVStrings *>(table_out.get_column(i)->data);
//				NVCategory * category = NVCategory::create_from_strings(*strs);
//				std::string column_name(table_out.get_column(i)->col_name);
//				columns_out[idx[i]].create_gdf_column(category, table_out.get_column(i)->size, column_name);
//				gdf_column_free(table_out.get_column(i));
//			} else {
//				columns_out[idx[i]].create_gdf_column(table_out.get_column(i));
//			}
//		}
	}
}


void csv_parser::parse_schema(
	std::vector<std::shared_ptr<arrow::io::RandomAccessFile>> files, ral::io::Schema & schema) {
	
	// TODO percy cudf0.12 port cudf::column and io stuff
//	cudf::table table_out = read_csv_arg_arrow(csv_arg, files[0], true);

//	assert(table_out.num_columns() > 0);

//	for(size_t i = 0; i < table_out.num_columns(); i++) {
//		gdf_column_cpp c;
//		c.create_gdf_column(table_out.get_column(i));
//		if(i < csv_arg.names.size())
//			c.set_name(csv_arg.names[i]);
//		schema.add_column(c, i);
//	}
}

} /* namespace io */
} /* namespace ral */
