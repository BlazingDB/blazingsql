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
#include <cudf/io/functions.hpp>
#include <blazingdb/io/Library/Logging/Logger.h>

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


cudf::experimental::io::read_csv_args das_BOT(std::shared_ptr<arrow::io::RandomAccessFile> file, cudf::csv_read_arg old) {
	cudf::experimental::io::read_csv_args readerArg{cudf::experimental::io::source_info(file)};

	// TODO rommel One of: `none`, `infer`, `bz2`, `gz`, `xz`, `zip`; default detects from file extension
	readerArg.compression = cudf::experimental::io::compression_type::NONE;
	readerArg.lineterminator = old.lineterminator;
	readerArg.delimiter = old.delimiter;
	readerArg.windowslinetermination = old.windowslinetermination;
	readerArg.delim_whitespace = old.delim_whitespace;
	readerArg.skipinitialspace = old.skipinitialspace;
	readerArg.skip_blank_lines = old.skip_blank_lines;
	readerArg.nrows = old.nrows;
	readerArg.skiprows = old.skiprows;
	readerArg.skipfooter = old.skipfooter;
//} else {
	// NOTE check this default value percy c.cordova
	readerArg.header = -1;
//}
	readerArg.names = old.names;
	readerArg.dtype = old.dtype;
	readerArg.use_cols_indexes = old.use_cols_indexes;
	readerArg.use_cols_names = old.use_cols_names;
	readerArg.true_values = old.true_values;
	readerArg.false_values = old.false_values;
	readerArg.na_values = old.na_values;
	readerArg.keep_default_na = old.keep_default_na;
	readerArg.na_filter = old.na_filter;
	readerArg.prefix = old.prefix;
	readerArg.mangle_dupe_cols = old.mangle_dupe_cols;
	readerArg.dayfirst = old.dayfirst;
	readerArg.thousands = old.thousands;
	readerArg.decimal = old.decimal;
	readerArg.comment = old.comment;
	readerArg.quotechar = old.quotechar;
// if (in("quoting", args)) {
//    readerArg.quoting = old.quoting"]
	readerArg.doublequote = old.doublequote;
	readerArg.byte_range_offset = old.byte_range_offset;
	readerArg.byte_range_size = old.byte_range_size;
	// TODO
	// readerArg.out_time_unit = old.out_time_unit"]
}
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
			create_empty_columns(schema.get_names(), schema.get_dtypes(), schema.get_time_units(), column_indices);
		return;
	}
	auto csv_arg = this->csv_arg;
	if(column_indices.size() > 0) {
		// copy column_indices into use_col_indexes (at the moment is ordered only)
		csv_arg.use_cols_indexes.resize(column_indices.size());
		csv_arg.use_cols_indexes.assign(column_indices.begin(), column_indices.end());

		cudf::experimental::io::read_csv_args csv_args = das_BOT(file, csv_arg);
		cudf::experimental::io::table_with_metadata table_and_metadata = cudf::experimental::io::read_csv(csv_args);

		if(table_and_metadata.tbl->num_columns() <= 0)
			Library::Logging::Logger().logWarn("parquet_parser::parse no columns were read");

		// column_indices may be requested in a specific order (not necessarily sorted), but read_csv will output the
		// columns in the sorted order, so we need to put them back into the order we want
		std::vector<size_t> idx(column_indices.size());
		std::iota(idx.begin(), idx.end(), 0);
		// sort indexes based on comparing values in column_indices
		std::sort(idx.begin(), idx.end(), [&column_indices](size_t i1, size_t i2) {
			return column_indices[i1] < column_indices[i2];
		});

		columns_out.resize(column_indices.size());
		for(size_t i = 0; i < table_and_metadata.tbl->num_columns(); i++) {
			if(table_and_metadata.tbl->get_column(i).type().id() == cudf::type_id::STRING) {
//				NVStrings * strs = static_cast<NVStrings *>(table_out.get_column(i)->data);
//				NVCategory * category = NVCategory::create_from_strings(*strs);
//				std::string column_name(table_out.get_column(i)->col_name);
//				columns_out[idx[i]].create_gdf_column(category, table_out.get_column(i)->size, column_name);
//				gdf_column_free(table_out.get_column(i));
			} else {
				columns_out[i].create_gdf_column(table_and_metadata.tbl->get_column(i), table_and_metadata.metadata.column_names[i]);
			}
		}
	}
}


void csv_parser::parse_schema(
	std::vector<std::shared_ptr<arrow::io::RandomAccessFile>> files, ral::io::Schema & schema) {
	cudf::table table_out = read_csv_arg_arrow(csv_arg, files[0], true);

	assert(table_out.num_columns() > 0);

	for(size_t i = 0; i < table_out.num_columns(); i++) {
		gdf_column_cpp c;
		c.create_gdf_column(table_out.get_column(i));
		if(i < csv_arg.names.size())
			c.set_name(csv_arg.names[i]);
		schema.add_column(c, i);
	}
}

} /* namespace io */
} /* namespace ral */
