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

#include <blazingdb/io/Library/Logging/Logger.h>

#define checkError(error, txt)                                                                                         \
	if(error != GDF_SUCCESS) {                                                                                         \
		std::cerr << "ERROR:  " << error << "  in " << txt << std::endl;                                               \
		return error;                                                                                                  \
	}

namespace ral {
namespace io {

csv_parser::csv_parser(cudf::io::csv_reader_options args_) : csv_args{args_} {}

csv_parser::~csv_parser() {}

cudf::io::table_with_metadata read_csv_arg_arrow(cudf::io::csv_reader_options args,
	std::shared_ptr<arrow::io::RandomAccessFile> arrow_file_handle,
	bool first_row_only = false) {

	auto arrow_source = cudf::io::arrow_io_source{arrow_file_handle};
	args = cudf::io::csv_reader_options::builder(cudf::io::source_info{&arrow_source});

	int64_t num_bytes = arrow_file_handle->GetSize().ValueOrDie();

	// lets only read up to 48192 bytes. We are assuming that a full row will always be less than that
	if(first_row_only && num_bytes > 48192) {
		args.set_byte_range_size(48192);
		args.set_nrows(1);
		args.set_skipfooter(0);
	}

	if(args.get_nrows() != -1)
		args.set_skipfooter(0);

	cudf::io::table_with_metadata table_out = cudf::io::read_csv(args);

	arrow_file_handle->Close();

	return std::move(table_out);
}


std::unique_ptr<ral::frame::BlazingTable> csv_parser::parse_batch(
	std::shared_ptr<arrow::io::RandomAccessFile> file,
	const Schema & schema,
	std::vector<int> column_indices,
	std::vector<cudf::size_type> row_groups) {

	if(file == nullptr) {
		return schema.makeEmptyBlazingTable(column_indices);
	}

	cudf::io::csv_reader_options new_csv_arg = this->csv_args;
	if(column_indices.size() > 0) {
		// copy column_indices into use_col_indexes (at the moment is ordered only)
		new_csv_arg.set_use_cols_indexes(column_indices);

		cudf::io::table_with_metadata csv_table = read_csv_arg_arrow(new_csv_arg, file);

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
	std::shared_ptr<arrow::io::RandomAccessFile> file, ral::io::Schema & schema) {

	cudf::io::table_with_metadata table_out = read_csv_arg_arrow(csv_args, file, true);

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
