/*
 * GDFParser.cpp
 *
 *  Created on: Apr 30, 2019
 *      Author: felipe
 */

#include "ArrowParser.h"

#include <iostream>

#include "arrow/api.h"
#include "arrow/array.h"
#include "arrow/table.h"
#include "arrow/record_batch.h"

namespace ral {
namespace io {

arrow_parser::arrow_parser(std::shared_ptr< arrow::Table > table):  table(table) {
	// TODO Auto-generated constructor stub

	std::cout<<"the total num rows is "<<table->num_rows()<<std::endl;
	// WSM TODO table_schema news to be newed up and copy in the properties
}

arrow_parser::~arrow_parser() {}

// TODO: cordova erase this code when the new GDF parse works well with the new API
// using UNIT TEST to check when it's ready
// void arrow_parser::parse(std::shared_ptr<arrow::io::RandomAccessFile> file,
// 		const std::string & user_readable_file_handle,
// 		std::vector<gdf_column_cpp> & columns_out,
// 		const Schema & schema,
// 		std::vector<size_t> column_indices_requested){

// 	if (column_indices_requested.size() == 0){ // including all columns by default
// 		column_indices_requested.resize(schema.get_num_columns());
// 		std::iota(column_indices_requested.begin(), column_indices_requested.end(), 0);
// 	}
// }

std::unique_ptr<ral::frame::BlazingTable> arrow_parser::parse(
	std::shared_ptr<arrow::io::RandomAccessFile> file,
	const std::string & user_readable_file_handle,
	const Schema & schema,
	std::vector<size_t> column_indices) {
		// TODO: cordova Implements the new ARROW parser with 0.12 API
	return nullptr;
}	

void arrow_parser::parse_schema(std::vector<std::shared_ptr<arrow::io::RandomAccessFile> > files,
		ral::io::Schema & schema){
	std::vector<std::string> names;
	std::vector<cudf::type_id> types;
}

}
}
