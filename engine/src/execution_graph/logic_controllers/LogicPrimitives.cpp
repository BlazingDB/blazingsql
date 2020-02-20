
#include "LogicPrimitives.h"

#include "cudf/column/column_factories.hpp"

namespace ral{

namespace frame{


BlazingTable::BlazingTable(	std::unique_ptr<CudfTable> table, const std::vector<std::string> & columnNames){

	std::vector<std::unique_ptr<CudfColumn>> columns_in = table->release();
	for (size_t i = 0; i < columns_in.size(); i++){
		columns.emplace_back(std::make_unique<BlazingColumnOwner>(std::move(columns_in[i])));
	}
	this->columnNames = columnNames;
}

BlazingTable::BlazingTable(const CudfTableView & table, const std::vector<std::string> & columnNames){
	for (size_t i = 0; i < table.num_columns(); i++){
		columns.emplace_back(std::make_unique<BlazingColumnView>(table.column(i)));
	}
	this->columnNames = columnNames;
}

CudfTableView BlazingTable::view() const{
	std::vector<CudfColumnView> column_views(columns.size());
	for (size_t i = 0; i < columns.size(); i++){
		column_views[i] = columns[i]->view();
	}
  	return CudfTableView(column_views);
}

std::vector<std::string> BlazingTable::names() const{
  return this->columnNames;
}

BlazingTableView BlazingTable::toBlazingTableView() const{
  return BlazingTableView(this->view(), this->columnNames);
}

std::unique_ptr<CudfTable> BlazingTable::releaseCudfTable() { 
	std::vector<std::unique_ptr<CudfColumn>> columns_out;
	for (size_t i = 0; i < columns.size(); i++){
		columns_out.emplace_back(columns[i]->release());
	}
	return std::make_unique<CudfTable>(std::move(columns_out));
}

std::vector<std::unique_ptr<BlazingColumn>> BlazingTable::releaseBlazingColumns() {
	return std::move(columns);
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

std::unique_ptr<ral::frame::BlazingTable> createEmptyBlazingTable(std::vector<cudf::type_id> column_types,
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
	return std::make_unique<BlazingTable>(std::move(cudf_table), column_names);	
}

}
}
