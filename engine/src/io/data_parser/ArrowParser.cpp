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

	/*std::vector<cudf::size_type> indices;
	indices.reserve(column_indices.size());
	std::transform(
		column_indices.cbegin(), column_indices.cend(), std::back_inserter(indices), [](std::size_t x) { return x; });
	CudfTableView tableView = data_handle.table_view.view().select(indices);

	if(tableView.num_columns() <= 0) {
		Library::Logging::Logger().logWarn("gdf_parser::parse_batch no columns were read");
	}

	std::vector<std::string> column_names_out;
	column_names_out.resize(column_indices.size());

	// we need to output the same column names of tableView
	for (size_t i = 0; i < column_indices.size(); ++i) {
		size_t idx = column_indices[i];
		column_names_out[i] = data_handle.table_view.names()[idx];
	}

	return std::make_unique<ral::frame::BlazingTable>(tableView, column_names_out);*/
}

void arrow_parser::parse_schema(std::shared_ptr<arrow::io::RandomAccessFile> /*file*/,
		ral::io::Schema &  /*schema*/){
	std::vector<std::string> names;
	std::vector<cudf::type_id> types;
}

}
}
