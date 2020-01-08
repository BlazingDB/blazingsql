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

orc_parser::orc_parser(cudf::orc_read_arg orc_arg_) : orc_arg{orc_arg_} {}

orc_parser::orc_parser(cudf::experimental::io::read_orc_args orc_arg_) : orc_args{orc_arg_} {}

orc_parser::~orc_parser() {
	// TODO Auto-generated destructor stub
}

cudf::experimental::io::table_with_metadata get_new_orc(cudf::orc_read_arg old_orc, 
	std::shared_ptr<arrow::io::RandomAccessFile> file,
	bool first_row_only = false){

	cudf::experimental::io::read_orc_args new_orc_args{cudf::experimental::io::source_info(file)};

	// copy old orc struct to the new one
	old_orc.columns = new_orc_args.columns;
	old_orc.stripe = new_orc_args.stripe;
	old_orc.skip_rows = new_orc_args.skip_rows;
	old_orc.num_rows = new_orc_args.num_rows;
	old_orc.use_index = new_orc_args.use_index;
	old_orc.use_np_dtypes = new_orc_args.use_np_dtypes;
	old_orc.decimals_as_float = new_orc_args.decimals_as_float;
	old_orc.forced_decimals_scale = new_orc_args.forced_decimals_scale;
	// this param is different for both versions
	// old_orc.timestamp_unit = new_orc_args.timestamp_type;
	
	if (first_row_only) 
		new_orc_args.num_rows = 1;

	cudf::experimental::io::table_with_metadata table_orc = cudf::experimental::io::read_orc(new_orc_args);
	assert(table_orc.tbl->num_columns() > 0);

	return table_orc;
}

void orc_parser::parse(std::shared_ptr<arrow::io::RandomAccessFile> file,
	const std::string & user_readable_file_handle,
	std::vector<gdf_column_cpp> & columns_out, // TODO: should not use anymore gdf_column_cpp
	const Schema & schema,
	std::vector<size_t> column_indices) {

	// TODO percy cudf0.12 port cudf::column and io stuff
	if(column_indices.size() == 0) {  // including all columns by default
		column_indices.resize(schema.get_num_columns());
		std::iota(column_indices.begin(), column_indices.end(), 0);
	}

	if(file == nullptr) {
		// TODO columns_out should change
		//columns_out =	create_empty_columns(schema.get_names(), schema.get_dtypes(), column_indices);
		return;
	}
	auto orc_arg = this->orc_arg;  // force a copy
	if(column_indices.size() > 0) {

		for(size_t column_i = 0; column_i < column_indices.size(); column_i++) {
			orc_arg.columns[column_i] = schema.get_name(column_indices[column_i]);
		}

		cudf::experimental::io::table_with_metadata orc_table = get_new_orc(orc_arg, file);

		if(orc_table.tbl->num_columns() <= 0)
			Library::Logging::Logger().logWarn("csv_parser::parse no columns were read");

		// TODO columns_out should change (gdf_column_cpp)
//		columns_out.resize(column_indices.size());
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
	}
}


void orc_parser::parse_schema(
	std::vector<std::shared_ptr<arrow::io::RandomAccessFile>> files, ral::io::Schema & schema) {
	
	// TODO percy cudf0.12 port cudf::column and io stuff
	cudf::experimental::io::table_with_metadata table_out = get_new_orc(orc_arg, files[0], true);

	for(size_t i = 0; i < table_out.tbl->num_columns() ; i++) {
		std::string name = "";
		if (orc_arg.columns.size() > 0 && i < orc_arg.columns.size())
			name = orc_arg.columns[i];
		cudf::type_id type = table_out.tbl->get_column(i).type().id();
		size_t file_index = i;
		bool is_in_file = true;
		schema.add_column(name, type, file_index, is_in_file);
	}
}

} /* namespace io */
} /* namespace ral */
