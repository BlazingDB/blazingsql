/*
 * GDFParser.cpp
 *
 *  Created on: Apr 30, 2019
 *      Author: felipe
 */

#include "ArrowParser.h"

#include "arrow/api.h"

namespace ral {
namespace io {

arrow_parser::arrow_parser(std::shared_ptr< arrow::Table > table):  table(table) {
	// TODO Auto-generated constructor stub

	std::cout<<"the total num rows is "<<table->num_rows()<<std::endl;
	// WSM TODO table_schema news to be newed up and copy in the properties
}

arrow_parser::~arrow_parser() {}

void arrow_parser::parse_schema(ral::io::data_handle /*handle*/,
		ral::io::Schema &  /*schema*/){
	std::vector<std::string> names;
	std::vector<cudf::type_id> types;
}

}
}
