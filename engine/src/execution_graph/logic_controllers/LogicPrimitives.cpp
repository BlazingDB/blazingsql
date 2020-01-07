
#include "LogicPrimitives.h"
namespace ral{

namespace frame{


BlazingTable::BlazingTable(
  std::unique_ptr<CudfTable> table,
  std::vector<std::string> columnNames)
  : table(std::move(table)), columnNames(columnNames){

}

CudfTableView BlazingTable::view(){
  return this->table->view();
}

std::vector<std::string> BlazingTable::names(){
  return this->columnNames;
}

BlazingTableView::BlazingTableView(
  CudfTableView table,
  std::vector<std::string> columnNames)
  : table(table), columnNames(columnNames){
  
}

CudfTableView BlazingTableView::view(){
  return this->table;
}

std::vector<std::string> BlazingTableView::names(){
  return this->columnNames;
}


}
}
