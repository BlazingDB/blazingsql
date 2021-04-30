#include "ArrowParser.h"
#include <cudf/interop.hpp>
#include "arrow/api.h"

namespace ral {
namespace io {

// arrow_parser::arrow_parser(std::shared_ptr<arrow::Table> table):  table(table) {
// 	// TODO Auto-generated constructor stub

// 	std::cout<<"the total num rows is "<<table->num_rows()<<std::endl;
// 	// WSM TODO table_schema news to be newed up and copy in the properties
// }

arrow_parser::arrow_parser() {}

arrow_parser::~arrow_parser() {}

std::unique_ptr<ral::frame::BlazingTable> arrow_parser::parse_batch(
		ral::io::data_handle data_handle,
		const Schema & schema,
		std::vector<int> column_indices,
		std::vector<cudf::size_type> /*row_groups*/){

	if(schema.get_num_columns() == 0) {
		return nullptr;
	}

	return std::make_unique<ral::frame::BlazingTable>(std::move(cudf::from_arrow(*data_handle.arrow_table)), data_handle.arrow_table->ColumnNames());
}

void arrow_parser::parse_schema(ral::io::data_handle /*handle*/,
		ral::io::Schema &  /*schema*/){
	std::vector<std::string> names;
	std::vector<cudf::type_id> types;
}

}
}
