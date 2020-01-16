#include "OrcParser.h"
#include <blazingdb/io/Util/StringUtil.h>
#include <cudf/legacy/column.hpp>
#include <cudf/legacy/io_functions.hpp>

#include <arrow/io/file.h>

#include <thread>

#include <GDFColumn.cuh>
#include <GDFCounter.cuh>

#include "../Schema.h"
#include "io/data_parser/ParserUtil.h"

#include <cudf/table/table.hpp>
#include <cudf/io/functions.hpp>
#include <blazingdb/io/Library/Logging/Logger.h>
#include <cudf/types.hpp>

#include <numeric>

namespace ral {
namespace io {

orc_parser::orc_parser(cudf_io::read_orc_args arg_) : orc_args{arg_} {}

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
	const std::string & user_readable_file_handle, // TODO where is this param used?
	const Schema & schema,
	std::vector<size_t> column_indices) {

	// including all columns by default
	if(column_indices.size() == 0) {
		column_indices.resize(schema.get_num_columns());
		std::iota(column_indices.begin(), column_indices.end(), 0);
	}

	if(file == nullptr) {
		// TODO columns_out not exist anymore
		//columns_out =	create_empty_columns(schema.get_names(), schema.get_dtypes(), column_indices);
		return nullptr;
	}
	
	cudf_io::read_orc_args orc_args = this->orc_args;
	if(column_indices.size() > 0) {

		for(size_t column_i = 0; column_i < column_indices.size(); column_i++) {
			orc_args.columns[column_i] = schema.get_name(column_indices[column_i]);
		}

		cudf_io::table_with_metadata orc_table = get_new_orc(orc_args, file);

		if(orc_table.tbl->num_columns() <= 0)
			Library::Logging::Logger().logWarn("orc_parser::parse no columns were read");

		// TODO columns_out should change (gdf_column_cpp)
		for(size_t i = 0; i < orc_table.tbl->num_columns(); i++) {
			if(orc_table.tbl->get_column(i).type().id() == cudf::type_id::STRING) {
//				NVStrings * strs = static_cast<NVStrings *>(table_out.get_column(i)->data);
//				NVCategory * category = NVCategory::create_from_strings(*strs);
//				std::string column_name(table_out.get_column(i)->col_name);
//				columns_out[i].create_gdf_column(category, table_out.get_column(i)->size, column_name);
//				gdf_column_free(table_out.get_column(i));
			} else {
				//TODO create_gdf_column anymore
//				columns_out[i].create_gdf_column(table_out.get_column(i));
			}
		}
		return std::make_unique<ral::frame::BlazingTable>(std::move(orc_table.tbl), orc_table.metadata.column_names);
	}
	return nullptr;
}

void orc_parser::parse_schema(
	std::vector<std::shared_ptr<arrow::io::RandomAccessFile>> files, ral::io::Schema & schema) {
	
	cudf_io::table_with_metadata table_out = get_new_orc(orc_args, files[0], true);
	assert(table_out.tbl->num_columns() > 0);

	for(size_t i = 0; i < table_out.tbl->num_columns() ; i++) {
		std::string name = "";
		if (i < orc_args.columns.size())
			name = orc_args.columns[i];
		cudf::type_id type = table_out.tbl->get_column(i).type().id();
		size_t file_index = i;
		bool is_in_file = true;
		schema.add_column(name, type, file_index, is_in_file);
	}
}

} /* namespace io */
} /* namespace ral */
