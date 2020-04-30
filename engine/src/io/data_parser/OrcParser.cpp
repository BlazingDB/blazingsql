#include "OrcParser.h"

#include <arrow/io/file.h>

#include <blazingdb/io/Library/Logging/Logger.h>

#include <numeric>

namespace ral {
namespace io {

orc_parser::orc_parser(cudf::experimental::io::read_orc_args arg_) : orc_args{arg_} {}

orc_parser::~orc_parser() {
	// TODO Auto-generated destructor stub
}

cudf_io::table_with_metadata get_new_orc(cudf_io::read_orc_args orc_arg, 
	std::shared_ptr<arrow::io::RandomAccessFile> arrow_file_handle,
	bool first_row_only = false){

	orc_arg.source = cudf_io::source_info(arrow_file_handle);
	
	if (first_row_only) 
		orc_arg.num_rows = 1;

	cudf_io::table_with_metadata table_out = cudf_io::read_orc(orc_arg);

	arrow_file_handle->Close();

	return std::move(table_out);
}

std::unique_ptr<ral::frame::BlazingTable> orc_parser::parse(
	std::shared_ptr<arrow::io::RandomAccessFile> file,
	const Schema & schema,
	std::vector<size_t> column_indices) {

	if(file == nullptr) {
		return nullptr;
	}
	
	cudf::experimental::io::read_orc_args new_orc_args = this->orc_args;
	if(column_indices.size() > 0) {
		new_orc_args.columns.resize(column_indices.size());
		
		for(size_t column_i = 0; column_i < column_indices.size(); column_i++) {
			new_orc_args.columns[column_i] = schema.get_name(column_indices[column_i]);
		}

		cudf_io::table_with_metadata orc_table = get_new_orc(new_orc_args, file);

		if(orc_table.tbl->num_columns() <= 0)
			Library::Logging::Logger().logWarn("orc_parser::parse no columns were read");

		return std::make_unique<ral::frame::BlazingTable>(std::move(orc_table.tbl), orc_table.metadata.column_names);		
	}
	return nullptr;
}

std::unique_ptr<ral::frame::BlazingTable> orc_parser::parse_batch(
	std::shared_ptr<arrow::io::RandomAccessFile> file,
	const Schema & schema,
	std::vector<size_t> column_indices,
	cudf::size_type stripe)
{
	if(file == nullptr) {
		return schema.makeEmptyBlazingTable(column_indices);
	}
	if(column_indices.size() > 0) {
		// Fill data to orc_args
		cudf_io::read_orc_args orc_args{cudf_io::source_info{file}};

		orc_args.columns.resize(column_indices.size());

		for(size_t column_i = 0; column_i < column_indices.size(); column_i++) {
			orc_args.columns[column_i] = schema.get_name(column_indices[column_i]);
		}

		orc_args.stripe = stripe;

		auto result = cudf_io::read_orc(orc_args);
		return std::make_unique<ral::frame::BlazingTable>(std::move(result.tbl), result.metadata.column_names);
	}
	return nullptr;
}

void orc_parser::parse_schema(
	std::shared_ptr<arrow::io::RandomAccessFile> file, ral::io::Schema & schema) {
	
	cudf_io::table_with_metadata table_out = get_new_orc(orc_args, file, true);
	
	for(cudf::size_type i = 0; i < table_out.tbl->num_columns() ; i++) {
		std::string name = table_out.metadata.column_names[i];
		cudf::type_id type = table_out.tbl->get_column(i).type().id();
		size_t file_index = i;
		bool is_in_file = true;
		schema.add_column(name, type, file_index, is_in_file);
	}
}

} /* namespace io */
} /* namespace ral */
