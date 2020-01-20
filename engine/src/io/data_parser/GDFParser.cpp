/*
 * GDFParser.cpp
 *
 *  Created on: Apr 30, 2019
 *      Author: felipe
 */

#include <numeric>

#include "GDFParser.h"
#include <blazingdb/io/Library/Logging/Logger.h>

namespace ral {
namespace io {

gdf_parser::gdf_parser(std::vector<cudf::column *> columns_, std::vector<std::string> names_) {
	this->columns = columns_;
	this->names = names_;
}

gdf_parser::~gdf_parser() {}

std::unique_ptr<ral::frame::BlazingTable> gdf_parser::parse(
	std::shared_ptr<arrow::io::RandomAccessFile> file,
	const std::string & user_readable_file_handle,
	const Schema & schema,
	std::vector<size_t> column_indices) {

	if (schema.get_num_columns() == 0) {
		// TODO: See how returns a empty table
		return nullptr;	
	}

	if (column_indices.size() == 0) { // including all columns by default
		column_indices.resize(schema.get_num_columns());
		std::iota(column_indices.begin(), column_indices.end(), 0);
	}
	
	std::vector< std::unique_ptr<cudf::column> > cudf_columns;
	for (size_t column_i = 0; column_i < column_indices.size(); column_i++) {
		cudf_columns.emplace_back( std::move(this->columns[column_i]) );
	}

	std::unique_ptr<CudfTable> table_gdf = std::make_unique<CudfTable>(std::move(cudf_columns));
	
	if (table_gdf->num_columns() <= 0)
		Library::Logging::Logger().logWarn("gdf_parser::parse no columns were read");

	return std::make_unique<ral::frame::BlazingTable>(std::move(table_gdf), this->names);
}

void gdf_parser::parse_schema(
	std::vector<std::shared_ptr<arrow::io::RandomAccessFile>> files, ral::io::Schema & schema) {
}


}  // namespace io
}  // namespace ral
