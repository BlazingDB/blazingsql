#include <arrow/io/file.h>
#include <arrow/status.h>
#include <blazingdb/io/Library/Logging/Logger.h>
#include <blazingdb/io/Util/StringUtil.h>
#include <numeric>
#include <thread>

#include "JSONParser.h"
#include "../Schema.h"

namespace ral {
namespace io {

json_parser::json_parser(cudf::experimental::io::read_json_args args) : args(args) {}

json_parser::~json_parser() {
	// TODO Auto-generated destructor stub
}

cudf::experimental::io::table_with_metadata read_json_file(
	cudf::experimental::io::read_json_args args,
	std::shared_ptr<arrow::io::RandomAccessFile> arrow_file_handle,
	bool first_row_only = false)
{
	args.source = cudf::experimental::io::source_info(arrow_file_handle);

	if(first_row_only) {
		int64_t num_bytes;
		arrow_file_handle->GetSize(&num_bytes);
		
		if(num_bytes > 48192) {
			// lets only read up to 8192 bytes. We are assuming that a full row will always be less than that
			num_bytes = 48192;
		}

		args.byte_range_offset = 0;
		args.byte_range_size = num_bytes;
	}

	auto table_and_metadata = cudf::experimental::io::read_json(args);

	arrow_file_handle->Close();

	return std::move(table_and_metadata);
}

std::unique_ptr<ral::frame::BlazingTable> json_parser::parse(
	std::shared_ptr<arrow::io::RandomAccessFile> file,
	const std::string & user_readable_file_handle,
	const Schema & schema,
	std::vector<size_t> column_indices) {

	if(file == nullptr) {
		return nullptr;
	}

	cudf::experimental::io::read_json_args new_json_args = args;

	// All json columns are be read
	auto table_and_metadata = read_json_file(args, file);

	if(table_and_metadata.tbl->num_columns() <= 0)
		Library::Logging::Logger().logWarn("json_parser::parse no columns were read");

	auto columns = table_and_metadata.tbl->release();
	auto column_names = std::move(table_and_metadata.metadata.column_names);

	// We just need the columns in column_indices
	std::vector<std::unique_ptr<cudf::column>> selected_columns;
	selected_columns.reserve(column_indices.size());
	std::vector<std::string> selected_column_names;
	selected_column_names.reserve(column_indices.size());
	for (auto &&i : column_indices) {
		selected_columns.push_back(std::move(columns[i]));
		selected_column_names.push_back(std::move(column_names[i]));
	}

	return std::make_unique<ral::frame::BlazingTable>(std::make_unique<cudf::experimental::table>(std::move(selected_columns)), selected_column_names);	
}

void json_parser::parse_schema(
	std::vector<std::shared_ptr<arrow::io::RandomAccessFile>> files, ral::io::Schema & schema) {

	auto table_and_metadata = read_json_file(args, files[0], true);
	assert(table_and_metadata.tbl->num_columns() > 0);

	for(auto i = 0; i < table_and_metadata.tbl->num_columns(); i++) {
		std::string name = table_and_metadata.metadata.column_names[i];
		cudf::type_id type = table_and_metadata.tbl->get_column(i).type().id();
		size_t file_index = i;
		bool is_in_file = true;
		schema.add_column(name, type, file_index, is_in_file);
	}
}

} /* namespace io */
} /* namespace ral */
