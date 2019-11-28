#include "OrcParser.h"
#include <cudf/legacy/io_functions.hpp>
#include <cudf/legacy/column.hpp>
#include <blazingdb/io/Util/StringUtil.h>

#include <arrow/io/file.h>

#include <thread>

#include <GDFColumn.cuh>
#include <GDFCounter.cuh>

#include "../Schema.h"
#include "io/data_parser/ParserUtil.h"

#include <numeric>

namespace ral {
namespace io {

orc_parser::orc_parser(cudf::orc_read_arg orc_args) : orc_args{orc_args} {
}

orc_parser::~orc_parser() {
	// TODO Auto-generated destructor stub
}

void orc_parser::parse(std::shared_ptr<arrow::io::RandomAccessFile> file,
	const std::string & user_readable_file_handle,
	std::vector<gdf_column_cpp> & columns_out,
	const Schema & schema,
	std::vector<size_t> column_indices){
	//std::cout<<std::endl<<"schema.get_num_columns()=" <<schema.get_num_columns()<<std::endl;
	if (column_indices.size() == 0){ // including all columns by default
		column_indices.resize(schema.get_num_columns());
		std::iota(column_indices.begin(), column_indices.end(), 0);
	}
	//std::cout<<"we have "<< column_indices.size()<<std::endl;
	for(int i = 0; i < column_indices.size(); i++){
		//std::cout<<"index "<<column_indices[i]<<std::endl;
	}
	if (file == nullptr){
		columns_out = create_empty_columns(schema.get_names(), schema.get_dtypes(), schema.get_time_units(), column_indices);
		return;
	}
	auto orc_args = this->orc_args; //force a copy
	if (column_indices.size() > 0){
		orc_args.source = cudf::source_info(file);
		orc_args.columns.resize(column_indices.size());

		for (size_t column_i = 0; column_i < column_indices.size(); column_i++) {
			orc_args.columns[column_i] = schema.get_name(column_indices[column_i]);
		}

		cudf::table table_out = cudf::read_orc(orc_args);
		assert(table_out.num_columns() > 0);

		columns_out.resize(column_indices.size());
		for(size_t i = 0; i < columns_out.size(); i++){

			if (table_out.get_column(i)->dtype == GDF_STRING){
				NVStrings* strs = static_cast<NVStrings*>(table_out.get_column(i)->data);
				NVCategory* category = NVCategory::create_from_strings(*strs);
				std::string column_name(table_out.get_column(i)->col_name);
				columns_out[i].create_gdf_column(category, table_out.get_column(i)->size, column_name);
				gdf_column_free(table_out.get_column(i));
			} else {
				columns_out[i].create_gdf_column(table_out.get_column(i));
			}
		}
	}
}


void orc_parser::parse_schema(std::vector<std::shared_ptr<arrow::io::RandomAccessFile> > files, ral::io::Schema & schema_out)  {
	orc_args.source = cudf::source_info(files[0]);
	orc_args.num_rows = 1;

	cudf::table table_out = cudf::read_orc(orc_args);
	assert(table_out.num_columns() > 0);

	for(size_t i = 0; i < table_out.num_columns(); i++ ){
		gdf_column_cpp c;
		c.create_gdf_column(table_out.get_column(i));
		c.set_name(table_out.get_column(i)->col_name);
		schema_out.add_column(c,i);
	}
}

} /* namespace io */
} /* namespace ral */
