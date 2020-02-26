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

gdf_parser::gdf_parser(frame::BlazingTableView blazingTableView) : blazingTableView_{blazingTableView} {}

gdf_parser::~gdf_parser() {}

std::unique_ptr<ral::frame::BlazingTable> gdf_parser::parse(std::shared_ptr<arrow::io::RandomAccessFile> file,
	const std::string & user_readable_file_handle,
	const Schema & schema,
	std::vector<std::size_t> column_indices) {
	
	if(schema.get_num_columns() == 0) {
		return nullptr;
	}

	std::vector<cudf::size_type> indices;
	indices.reserve(column_indices.size());
	std::transform(
		column_indices.cbegin(), column_indices.cend(), std::back_inserter(indices), [](std::size_t x) { return x; });
	CudfTableView tableView = blazingTableView_.view().select(indices);

	if(tableView.num_columns() <= 0) {
		Library::Logging::Logger().logWarn("gdf_parser::parse no columns were read");
	}
	
	std::vector<std::string> column_names_out;
	column_names_out.resize(column_indices.size());
	
	// we need to output the same column names of tableView
	for (size_t i = 0; i < column_indices.size(); ++i) {
		size_t idx = column_indices[i];
		column_names_out[i] = blazingTableView_.names()[idx];
	}
	
	return std::make_unique<ral::frame::BlazingTable>(tableView, column_names_out);
}

void gdf_parser::parse_schema(
	std::vector<std::shared_ptr<arrow::io::RandomAccessFile>> files, ral::io::Schema & schema) {}


}  // namespace io
}  // namespace ral
