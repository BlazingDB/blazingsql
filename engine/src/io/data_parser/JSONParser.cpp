/*
 * jsonParser.cpp
 *
 *  Created on: Nov 29, 2018
 *      Author: felipe
 */

#include "JSONParser.h"
#include <blazingdb/io/Util/StringUtil.h>
#include <cudf/legacy/column.hpp>
#include <cudf/legacy/io_functions.hpp>

#include <arrow/io/file.h>
#include <arrow/status.h>

#include <thread>

#include <GDFColumn.cuh>
#include <GDFCounter.cuh>

#include "../Schema.h"
#include "io/data_parser/ParserUtil.h"

#include <numeric>

namespace ral {
namespace io {

json_parser::json_parser(cudf::json_read_arg args) : args(args) {}

json_parser::~json_parser() {
	// TODO Auto-generated destructor stub
}

/**
 * @brief read in a JSON file
 *
 * Read in a JSON file, extract all fields, and return a GDF (array of gdf_columns) using arrow interface
 **/

cudf::table read_json_arrow(std::shared_ptr<arrow::io::RandomAccessFile> arrow_file_handle,
	bool lines,
	cudf::json_read_arg args,
	bool first_row_only = false) {

	int64_t num_bytes;
	arrow_file_handle->GetSize(&num_bytes);

	// lets only read up to 8192 bytes. We are assuming that a full row will always be less than that
	if(first_row_only && num_bytes > 48192) 
		num_bytes = 48192;

	std::string buffer;
	buffer.resize(num_bytes);

	args.source = cudf::source_info(arrow_file_handle);

	if(first_row_only) {
		args.byte_range_offset = 0;
		args.byte_range_size = num_bytes;
	}

	cudf::table table_out = cudf::read_json(args);

	arrow_file_handle->Close();

	return table_out;
}

// Deprecated
void json_parser::parse(std::shared_ptr<arrow::io::RandomAccessFile> file,
	const std::string & user_readable_file_handle,
	std::vector<gdf_column_cpp> & columns_out,
	const Schema & schema,
	std::vector<size_t> column_indices) {

	if(column_indices.size() == 0) {  // including all columns by default
		column_indices.resize(schema.get_num_columns());
		std::iota(column_indices.begin(), column_indices.end(), 0);
	}

	if(file == nullptr) {
		// TODO columns_out should change
		//columns_out =
		//	create_empty_columns(schema.get_names(), schema.get_dtypes(), column_indices);
		return;
	}

	if(column_indices.size() > 0) {
		// NOTE: All json columns will be read, we need to delete the unselected columns
		// TODO percy cudf0.12 port cudf::column and io stuff
//		cudf::table table_out = read_json_arrow(file, this->args.lines, this->args);
//		assert(table_out.num_columns() > 0);

//		columns_out.resize(column_indices.size());
//		for(size_t sel_idx = 0; sel_idx < columns_out.size(); sel_idx++) {
//			if(table_out.get_column(column_indices[sel_idx])->dtype == GDF_STRING) {
//				NVStrings * strs = static_cast<NVStrings *>(table_out.get_column(column_indices[sel_idx])->data);
//				NVCategory * category = NVCategory::create_from_strings(*strs);
//				std::string column_name(table_out.get_column(column_indices[sel_idx])->col_name);
//				columns_out[sel_idx].create_gdf_column(
//					category, table_out.get_column(column_indices[sel_idx])->size, column_name);
//				gdf_column_free(table_out.get_column(column_indices[sel_idx]));
//			} else {
//				columns_out[sel_idx].create_gdf_column(table_out.get_column(column_indices[sel_idx]));
//			}
//		}

		// Releasing the unselected columns
		// TODO percy cudf0.12 port cudf::column and io stuff
//		for(size_t idx = 0; idx < table_out.num_columns() && column_indices.size() > 0; idx++) {
//			if(std::find(column_indices.begin(), column_indices.end(), idx) == column_indices.end()) {
//				gdf_column_free(table_out.get_column(idx));
//			}
//		}
	}
}


std::unique_ptr<ral::frame::BlazingTable> json_parser::parse(
	std::shared_ptr<arrow::io::RandomAccessFile> file,
	const std::string & user_readable_file_handle,
	const Schema & schema,
	std::vector<size_t> column_indices) {
			return nullptr;
}

void json_parser::parse_schema(
	std::vector<std::shared_ptr<arrow::io::RandomAccessFile>> files, ral::io::Schema & schema_out) {

	// TODO percy cudf0.12 port cudf::column and io stuff
	cudf::table table_out = read_json_arrow(files[0], this->args.lines, this->args, true);
	assert(table_out.num_columns() > 0);

	for(size_t i = 0; i < table_out.num_columns(); i++) {
		// TODO gdf_column_cpp should not used anymore, how host the name?
//		gdf_column_cpp c;
//		c.create_gdf_column(table_out.get_column(i));
//		c.set_name(table_out.get_column(i)->col_name);
//		schema_out.add_column(c, i);
	}
}

} /* namespace io */
} /* namespace ral */
