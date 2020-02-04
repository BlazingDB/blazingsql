//Recibir blazingTable
//Imprimir Nombre, Tamanho, Data

#include "DebuggingUtils.h"
#include <cudf/utilities/type_dispatcher.hpp>
#include <cudf/strings/string_view.cuh>

namespace ral {
namespace utilities {

std::string type_string(cudf::data_type dtype) {
	using namespace cudf;

	switch (dtype.id()) {
		case BOOL8: return "BOOL8";
		case INT8:  return "INT8";
		case INT16: return "INT16";
		case INT32: return "INT32";
		case INT64: return "INT64";
		case FLOAT32: return "FLOAT32";
		case FLOAT64: return "FLOAT64";
		case STRING:  return "STRING";
		case TIMESTAMP_DAYS: return "TIMESTAMP_DAYS";
		case TIMESTAMP_SECONDS: return "TIMESTAMP_SECONDS";
		case TIMESTAMP_MILLISECONDS: return "TIMESTAMP_MILLISECONDS";
		case TIMESTAMP_MICROSECONDS: return "TIMESTAMP_MICROSECONDS";
		case TIMESTAMP_NANOSECONDS: return "TIMESTAMP_NANOSECONDS";
		default: return "Unsupported type_id";
	}
}

void print_blazing_table_view(ral::frame::BlazingTableView table_view, const std::string table_name){
	std::cout<<"Table: "<<table_name<<std::endl;
	std::cout<<"\t"<<"Num Rows: "<<table_view.num_rows()<<std::endl;
	std::cout<<"\t"<<"Num Columns: "<<table_view.num_columns()<<std::endl;
	for(size_t col_idx=0; col_idx<table_view.num_columns(); col_idx++){
		std::string col_string;
		if (table_view.num_rows() > 0){
			col_string = cudf::test::to_string(table_view.column(col_idx), "|");
		}
		std::cout<<"\t"<<table_view.names().at(col_idx)<<" ("<<"type: "<<type_string(table_view.column(col_idx).type())<<"): "<<col_string<<std::endl;		
	}
}

void print_blazing_table_view_schema(ral::frame::BlazingTableView table_view, const std::string table_name){
	std::cout<<"Table: "<<table_name<<std::endl;
	std::cout<<"\t"<<"Num Rows: "<<table_view.num_rows()<<std::endl;
	std::cout<<"\t"<<"Num Columns: "<<table_view.num_columns()<<std::endl;
	for(size_t col_idx=0; col_idx<table_view.num_columns(); col_idx++){
		std::cout<<"\t"<<table_view.names().at(col_idx)<<" ("<<"type: "<<type_string(table_view.column(col_idx).type())<<")"<<std::endl;
	}
}

}  // namespace utilities
}  // namespace ral