#include "OrcParser.h"

#include <arrow/io/file.h>

#include <blazingdb/io/Library/Logging/Logger.h>

#include <numeric>

namespace ral {
namespace io {

orc_parser::orc_parser(cudf::io::read_orc_args arg_) : orc_args{arg_} {}

orc_parser::~orc_parser() {
	// TODO Auto-generated destructor stub
}

cudf_io::table_with_metadata get_new_orc(cudf_io::read_orc_args orc_arg,
	std::shared_ptr<arrow::io::RandomAccessFile> arrow_file_handle,
	bool first_row_only = false){

	auto arrow_source = cudf_io::arrow_io_source{arrow_file_handle};
	orc_arg.source = cudf_io::source_info{&arrow_source};

	if (first_row_only)
		orc_arg.num_rows = 1;

	cudf_io::table_with_metadata table_out = cudf_io::read_orc(orc_arg);

	arrow_file_handle->Close();

	return std::move(table_out);
}

std::unique_ptr<ral::frame::BlazingTable> orc_parser::parse_batch(
	std::shared_ptr<arrow::io::RandomAccessFile> file,
	const Schema & schema,
	std::vector<size_t> column_indices,
	std::vector<cudf::size_type> row_groups)
{
	if(file == nullptr) {
		return schema.makeEmptyBlazingTable(column_indices);
	}
	if(column_indices.size() > 0) {
		// Fill data to orc_args
		auto arrow_source = cudf_io::arrow_io_source{file};
		orc_args.source = cudf_io::source_info{&arrow_source};

		orc_args.columns.resize(column_indices.size());

		for(size_t column_i = 0; column_i < column_indices.size(); column_i++) {
			orc_args.columns[column_i] = schema.get_name(column_indices[column_i]);
		}

		orc_args.stripes = row_groups;

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
