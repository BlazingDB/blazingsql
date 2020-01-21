
#include "LogicPrimitives.h"
namespace ral{

namespace frame{


BlazingTable::BlazingTable(
  std::unique_ptr<CudfTable> table,
  std::vector<std::string> columnNames)
  : table(std::move(table)), columnNames(columnNames){

}

CudfTableView BlazingTable::view() const{
  return this->table->view();
}

std::vector<std::string> BlazingTable::names() const{
  return this->columnNames;
}

BlazingTableView BlazingTable::toBlazingTableView() const{
  return BlazingTableView(this->table->view(), this->columnNames);
}

BlazingTableView::BlazingTableView(
  CudfTableView table,
  std::vector<std::string> columnNames)
  : table(table), columnNames(columnNames){

}

CudfTableView BlazingTableView::view() const{
  return this->table;
}

std::vector<std::string> BlazingTableView::names() const{
  return this->columnNames;
}

std::unique_ptr<BlazingTable> BlazingTableView::clone() const {
  std::unique_ptr<CudfTable> cudfTable = std::make_unique<CudfTable>(this->table);
  return std::make_unique<BlazingTable>(std::move(cudfTable), this->columnNames);
}


}
}
