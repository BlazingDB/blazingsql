

#include "LogicPrimitives.h"
#include <sys/stat.h>


#include <random>
#include <src/utilities/CommonOperations.h>

#include "cudf/column/column_factories.hpp"

namespace ral {

namespace frame{

BlazingTable::BlazingTable(std::vector<std::unique_ptr<BlazingColumn>> columns, const std::vector<std::string> & columnNames)
	: columns(std::move(columns)), columnNames(columnNames) {}


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
		columns_out.emplace_back(std::move(columns[i]->release()));
	}
	return std::make_unique<CudfTable>(std::move(columns_out));
}

std::vector<std::unique_ptr<BlazingColumn>> BlazingTable::releaseBlazingColumns() {
	return std::move(columns);
}


unsigned long long BlazingTable::sizeInBytes()
{
	unsigned long long total_size = 0UL;
	for(cudf::size_type i = 0; i < this->columns.size(); ++i) {
		auto & bz_column = this->columns[i];
		auto column =  bz_column->view();
		if(column.type().id() == cudf::type_id::STRING) {
			auto num_children = column.num_children();
			if(num_children == 2) {
				auto offsets_column = column.child(0);
				auto chars_column = column.child(1);

				total_size += chars_column.size();
				cudf::data_type offset_dtype(cudf::type_id::INT32);
				total_size += offsets_column.size() * cudf::size_of(offset_dtype);
				if(column.has_nulls()) {
					total_size += cudf::bitmask_allocation_size_bytes(column.size());
				}
			} else {
				// std::cerr << "string column with no children\n";
			}
		} else {
			total_size += column.size() * cudf::size_of(column.type());
			if(column.has_nulls()) {
				total_size += cudf::bitmask_allocation_size_bytes(column.size());
			}
		}
	}
	return total_size;
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

std::vector<std::unique_ptr<BlazingColumn>> BlazingTableView::toBlazingColumns() const{
	return cudfTableViewToBlazingColumns(this->table);
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

std::vector<std::unique_ptr<BlazingColumn>> cudfTableViewToBlazingColumns(const CudfTableView & table){
	std::vector<std::unique_ptr<BlazingColumn>> columns_out;
	for (size_t i = 0; i < table.num_columns(); i++){
		columns_out.emplace_back(std::make_unique<BlazingColumnView>(table.column(i)));
	}
	return columns_out;
}

} // end namespace frame
}  // namespace ral
