/*
 * GDFParser.cpp
 *
 *  Created on: Apr 30, 2019
 *      Author: felipe
 */

#include "GDFParser.h"

#include "io/data_parser/ParserUtil.h"

namespace ral {
namespace io {


gdf_parser::gdf_parser(TableSchema tableSchema) {
	// TODO Auto-generated constructor stub
	this->tableSchema = tableSchema;

	// WSM TODO table_schema news to be newed up and copy in the properties
}

gdf_parser::~gdf_parser() {}


void gdf_parser::parse(std::shared_ptr<arrow::io::RandomAccessFile> file,
	const std::string & user_readable_file_handle,
	std::vector<gdf_column_cpp> & columns_out,
	const Schema & schema,
	std::vector<size_t> column_indices_requested) {
	if(column_indices_requested.size() == 0) {  // including all columns by default
		column_indices_requested.resize(schema.get_num_columns());
		std::iota(column_indices_requested.begin(), column_indices_requested.end(), 0);
	}

	std::vector<gdf_column_cpp> columns;
	for(auto column_index : column_indices_requested) {
		const std::string column_name = this->tableSchema.names[column_index];

		auto column = this->tableSchema.columns[column_index];
		gdf_column_cpp col;
		if(column->dtype == GDF_STRING) {
			NVCategory * category = NVCategory::create_from_strings(*(NVStrings *) column->data);
			col.create_gdf_column(category, column->size, column_name);
		} else if(column->dtype == GDF_STRING_CATEGORY) {
			// The RAL can change the category during execution and the Python side won't
			// realize update the category so we have to make a copy
			// this shouldn't be not longer neccesary with the new cudf columns
			NVCategory * new_category = static_cast<NVCategory *>(column->dtype_info.category)->copy();
			col.create_gdf_column(new_category, column->size, column_name);
		} else {
			col.create_gdf_column(column, false);
		}

		columns.push_back(col);
	}
	columns_out = columns;
}

void gdf_parser::parse_schema(
	std::vector<std::shared_ptr<arrow::io::RandomAccessFile>> files, ral::io::Schema & schema) {
	std::vector<std::string> names;
	std::vector<gdf_dtype> types;
	std::vector<gdf_time_unit> time_units;

	std::for_each(
		this->tableSchema.columns.begin(), this->tableSchema.columns.end(), [&types, &time_units](gdf_column * column) {
			types.push_back(column->dtype);
			time_units.push_back(column->dtype_info.time_unit);
		});

	names = this->tableSchema.names;
	ral::io::Schema temp_schema(names, types, time_units);
	schema = temp_schema;
	// generate schema from message here
}


}  // namespace io
}  // namespace ral
