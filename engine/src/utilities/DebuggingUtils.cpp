#include <sstream>

#include "DebuggingUtils.h"
#include <cudf/utilities/type_dispatcher.hpp>
#include <cudf/strings/string_view.cuh>
#include <tests/utilities/column_utilities.hpp> 

namespace ral {
namespace utilities {

std::string type_string(cudf::data_type dtype) {
	using namespace cudf;

	switch (dtype.id()) {
		case type_id::INT8:  return "INT8";
		case type_id::INT16: return "INT16";
		case type_id::INT32: return "INT32";
		case type_id::INT64: return "INT64";
		case type_id::UINT8:  return "UINT8";
		case type_id::UINT16: return "UINT16";
		case type_id::UINT32: return "UINT32";
		case type_id::UINT64: return "UINT64";
		case type_id::FLOAT32: return "FLOAT32";
		case type_id::FLOAT64: return "FLOAT64";
		case type_id::BOOL8: return "BOOL8";
		case type_id::TIMESTAMP_DAYS: return "TIMESTAMP_DAYS";
		case type_id::TIMESTAMP_SECONDS: return "TIMESTAMP_SECONDS";
		case type_id::TIMESTAMP_MILLISECONDS: return "TIMESTAMP_MILLISECONDS";
		case type_id::TIMESTAMP_MICROSECONDS: return "TIMESTAMP_MICROSECONDS";
		case type_id::TIMESTAMP_NANOSECONDS: return "TIMESTAMP_NANOSECONDS";
		case type_id::DURATION_DAYS: return "DURATION_DAYS";
		case type_id::DURATION_SECONDS: return "DURATION_SECONDS";
		case type_id::DURATION_MILLISECONDS: return "DURATION_MILLISECONDS";
		case type_id::DURATION_MICROSECONDS: return "DURATION_MICROSECONDS";
		case type_id::DURATION_NANOSECONDS: return "DURATION_NANOSECONDS";
		case type_id::DICTIONARY32:  return "DICTIONARY32";
		case type_id::STRING:  return "STRING";
		case type_id::LIST:  return "LIST";
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
	std::cout<<blazing_table_view_schema_to_string(table_view, table_name);	
}

std::string blazing_table_view_schema_to_string(ral::frame::BlazingTableView table_view, const std::string table_name){
	std::ostringstream ostream;
	ostream <<"Table: "<<table_name<<std::endl;
	ostream<<"\t"<<"Num Rows: "<<table_view.num_rows()<<std::endl;
	ostream<<"\t"<<"Num Columns: "<<table_view.num_columns()<<std::endl;
	for(size_t col_idx=0; col_idx<table_view.num_columns(); col_idx++){
		ostream<<"\t"<<table_view.names().at(col_idx)<<" ("<<"type: "<<type_string(table_view.column(col_idx).type())<<")"<<std::endl;
	}
	return ostream.str();
}

// std::string cache_data_schema_to_string(ral::cache::CacheData * cache_data){
// 	std::ostringstream ostream;
// 	std::vector<std::string> cache_data_names = cache_data->names();
// 	std::vector<cudf::data_type> cache_data_types = cache_data->get_schema();
// 	ostream<<"Num Rows: "<<cache_data->num_rows()<<std::endl;
// 	ostream<<"\t"<<"Num Columns: "<<cache_data_names.size()<<std::endl;
// 	for(size_t col_idx=0; col_idx<cache_data_names.size(); col_idx++){
// 		ostream<<"\t"<<cache_data_names[col_idx]<<" ("<<"type: "<<type_string(cache_data_types[col_idx])<<")"<<std::endl;
// 	}
// 	return ostream.str();
// }

}  // namespace utilities
}  // namespace ral
