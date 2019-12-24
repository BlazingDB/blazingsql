/*
 * GDFParser.cpp
 *
 *  Created on: Apr 30, 2019
 *      Author: felipe
 */

#include "ArrowParser.h"
#include "ral-message.cuh"

#include "io/data_parser/ParserUtil.h"

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

arrow_parser::~arrow_parser() {

}


void arrow_parser::parse(std::shared_ptr<arrow::io::RandomAccessFile> file,
		const std::string & user_readable_file_handle,
		std::vector<gdf_column_cpp> & columns_out,
		const Schema & schema,
		std::vector<size_t> column_indices_requested){

	if (column_indices_requested.size() == 0){ // including all columns by default
		column_indices_requested.resize(schema.get_num_columns());
		std::iota(column_indices_requested.begin(), column_indices_requested.end(), 0);
	}
/*
	std::vector<gdf_column_cpp> column_indices_requested(column_indices_requested.size());
	for(auto column_index : column_indices_requested){
			auto column = table->column(column_index);
			if(schema.get_dtypes()[column_index] == GDF_STRING || schema.get_dtypes()[column_index] == GDF_CATEGORY){

			}else{
				column.create_gdf_column(schema.get_dtypes()[column_index],
					gdf_dtype_extra_info{TIME_UNIT_ms},
					num_rows,
					nullptr,
					ral::traits::get_dtype_size_in_bytes(scalar.dtype),
					name,
					true);
			}
	}

	arrow::TableBatchReader reader(*this->table);

	std::shared_ptr< arrow::RecordBatch > out;
	reader.ReadNext (&out);
	cudf::size_type row = 0;
	while(out != nullptr){

		for(auto column_index : column_indices_requested) {

		auto column = out->column(column_index);


		if(column->type->id() == arrow::Type::type::INT64 || column->type->id() == arrow::Type::type::UINT64){

		}else if(column->type->id() == arrow::Type::type::INT32 || column->type->id() == arrow::Type::type::UINT32){

		}else if(column->type->id() == arrow::Type::type::INT16 || column->type->id() == arrow::Type::type::UINT16){

		}else if(column->type->id() == arrow::Type::type::INT8 || column->type->id() == arrow::Type::type::UINT8){

		}


		}

		reader.ReadNext (&out);
	}
columns_out = columns;
*/

}

void arrow_parser::parse_schema(std::vector<std::shared_ptr<arrow::io::RandomAccessFile> > files,
		ral::io::Schema & schema){
	std::vector<std::string> names;
	std::vector<cudf::type_id> types;
	std::vector<gdf_time_unit> time_units;

}


}
}
