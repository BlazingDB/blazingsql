/*
 * GDFParser.cpp
 *
 *  Created on: Apr 30, 2019
 *      Author: felipe
 */

#include <numeric>

#include "GDFParser.h"
#include <blazingdb/io/Library/Logging/Logger.h>
#include <cudf/concatenate.hpp>

namespace ral {
namespace io {

gdf_parser::gdf_parser(std::vector<frame::BlazingTableView> blazingTableViews) : blazingTableViews_{blazingTableViews} {}

size_t gdf_parser::get_num_partitions(){
	return blazingTableViews_.size();
}

gdf_parser::~gdf_parser() {}

std::unique_ptr<ral::frame::BlazingTable> gdf_parser::parse(std::shared_ptr<arrow::io::RandomAccessFile> file,
	const Schema & schema,
	std::vector<std::size_t> column_indices) {
	
	if(schema.get_num_columns() == 0) {
		return nullptr;
	}

	std::vector<cudf::size_type> indices;
	indices.reserve(column_indices.size());
	std::transform(
		column_indices.cbegin(), column_indices.cend(), std::back_inserter(indices), [](std::size_t x) { return x; });
	
	std::vector<CudfTableView> tableViews;
	for (auto& tableView : blazingTableViews_){
		tableViews.push_back(tableView.view().select(indices));

		if(tableView.num_columns() <= 0) {
			Library::Logging::Logger().logWarn("gdf_parser::parse no columns were read");
		}
	}

	std::vector<std::string> column_names_out;
	column_names_out.resize(column_indices.size());
	
	// we need to output the same column names of tableView
	for (size_t i = 0; i < column_indices.size(); ++i) {
		size_t idx = column_indices[i];
		column_names_out[i] = blazingTableViews_[0].names()[idx];
	}
	
	std::unique_ptr<CudfTable> concatenated_tables = cudf::experimental::concatenate(tableViews);
	return std::make_unique<ral::frame::BlazingTable>(std::move(concatenated_tables), column_names_out);
}

std::unique_ptr<ral::frame::BlazingTable> gdf_parser::parse_batch(
		std::shared_ptr<arrow::io::RandomAccessFile> file,
		const Schema & schema,
		std::vector<size_t> column_indices,
		cudf::size_type partition){
	
	if(schema.get_num_columns() == 0) {
		return nullptr;
	}

	std::vector<cudf::size_type> indices;
	indices.reserve(column_indices.size());
	std::transform(
		column_indices.cbegin(), column_indices.cend(), std::back_inserter(indices), [](std::size_t x) { return x; });
	CudfTableView tableView = blazingTableViews_[partition].view().select(indices);

	if(tableView.num_columns() <= 0) {
		Library::Logging::Logger().logWarn("gdf_parser::parse_batch no columns were read");
	}

	std::vector<std::string> column_names_out;
	column_names_out.resize(column_indices.size());
	
	// we need to output the same column names of tableView
	for (size_t i = 0; i < column_indices.size(); ++i) {
		size_t idx = column_indices[i];
		column_names_out[i] = blazingTableViews_[0].names()[idx];
	}
	
	return std::make_unique<ral::frame::BlazingTable>(tableView, column_names_out);
}

void gdf_parser::parse_schema(
	std::shared_ptr<arrow::io::RandomAccessFile> file, ral::io::Schema & schema) {}


}  // namespace io
}  // namespace ral
