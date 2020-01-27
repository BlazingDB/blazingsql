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

ral::frame::TableViewPair gdf_parser::parse(std::shared_ptr<arrow::io::RandomAccessFile> file,
	const std::string & user_readable_file_handle,
	const Schema & schema,
	std::vector<std::size_t> column_indices) {
	// WSM TODO cudf0.12 revisit when Schema uses BlazingTableView
	if(schema.get_num_columns() == 0) {
		return std::make_pair(nullptr, frame::BlazingTableView());
	}

	if(column_indices.size() == 0) {  // including all columns by default
		column_indices.resize(schema.get_num_columns());
		std::iota(column_indices.begin(), column_indices.end(), 0);
	}

	std::vector<cudf::size_type> indices;
	indices.reserve(column_indices.size());
	std::transform(
		column_indices.cbegin(), column_indices.cend(), std::back_inserter(indices), [](std::size_t x) { return x; });
	CudfTableView tableView = blazingTableView_.view().select(indices);

	if(tableView.num_columns() <= 0) {
		Library::Logging::Logger().logWarn("gdf_parser::parse no columns were read");
	}

	return std::make_pair(nullptr, frame::BlazingTableView{tableView, blazingTableView_.names()});
}

void gdf_parser::parse_schema(
	std::vector<std::shared_ptr<arrow::io::RandomAccessFile>> files, ral::io::Schema & schema) {}


}  // namespace io
}  // namespace ral
