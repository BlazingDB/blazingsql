
#include "LogicPrimitives.h"

#include "cudf/column/column_factories.hpp"

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


BlazingTableView::BlazingTableView(){
  
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

TableViewPair createEmptyTableViewPair(std::vector<cudf::type_id> column_types,
									   std::vector<std::string> column_names) {
	std::vector< std::unique_ptr<cudf::column> > empty_columns;
	empty_columns.resize(column_types.size());
	for(int i = 0; i < column_types.size(); ++i) {
		cudf::type_id col_type = column_types[i];
		cudf::data_type dtype(col_type);
		std::unique_ptr<cudf::column> empty_column = cudf::make_empty_column(dtype);
		empty_columns[i] = std::move(empty_column);
	}
	
	std::unique_ptr<CudfTable> cudf_table = std::make_unique<CudfTable>(std::move(empty_columns));
	std::unique_ptr<BlazingTable> table = std::make_unique<BlazingTable>(std::move(cudf_table), column_names);
	
	TableViewPair ret;
	ret.first = std::move(table);
	ret.second = ret.first->toBlazingTableView();
	
	return ret;
}

}
}
