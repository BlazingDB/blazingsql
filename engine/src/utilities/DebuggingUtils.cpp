//Recibir blazingTable
//Imprimir Nombre, Tamanho, Data

#include "DebuggingUtils.h"
#include <cudf/utilities/type_dispatcher.hpp>
#include <cudf/strings/string_view.cuh>

namespace ral {
namespace utilities {

struct get_type_id_name {
  template <typename T>
  std::string operator()() {
    return typeid(T).name();
  }
};

void print_blazing_table_view(ral::frame::BlazingTableView table_view, const std::string table_name=""){
    std::cout<<"Table: "<<table_name<<std::endl;
	std::cout<<"\t"<<"Num Rows: "<<table_view.num_rows()<<std::endl;
	std::cout<<"\t"<<"Num Columns: "<<table_view.num_columns()<<std::endl;
	for(size_t col_idx=0; col_idx<table_view.num_columns(); col_idx++){
		std::string col_string = cudf::test::to_string(table_view.column(col_idx), "|");
		std::cout<<"\t"<<table_view.names().at(col_idx)<<" : "<<col_string<<std::endl;
        std::cout<<"\t Type: "<<cudf::experimental::type_dispatcher(table_view.column(col_idx).type(), get_type_id_name{});
	}
}

}  // namespace utilities
}  // namespace ral