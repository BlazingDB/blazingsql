//Recibir blazingTable
//Imprimir Nombre, Tamanho, Data

#include "DebuggingUtils.h"

namespace ral {
namespace utilities {

void print_blazing_table_view(ral::frame::BlazingTableView table_view){

	std::cout<<"Num Rows: "<<table_view.num_rows()<<std::endl;
	for(size_t col_idx=0; col_idx<table_view.num_columns(); col_idx++){
		std::string col_string = cudf::test::to_string(table_view.column(col_idx), "|");
			std::cout<<table_view.names().at(col_idx)<<col_idx<<" : "<<col_string<<std::endl;
	}
}

}  // namespace utilities
}  // namespace ral