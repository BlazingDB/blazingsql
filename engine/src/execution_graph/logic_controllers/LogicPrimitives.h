
#pragma once

#include <memory>
#include <vector>
#include <string>
#include "cudf/table/table.hpp"
#include "cudf/table/table_view.hpp"
#include "cudf/column/column_view.hpp"

typedef cudf::experimental::table CudfTable;
typedef cudf::table_view CudfTableView;
typedef cudf::column_view CudfColumnView;

namespace ral{

namespace frame{

	class BlazingTableView;

  	class BlazingTable{
  	public:
  		BlazingTable(std::unique_ptr<CudfTable> table,std::vector<std::string> columnNames);
  		CudfTableView view() const;
			cudf::size_type num_columns() const {
				return table->num_columns();
			}
			cudf::size_type num_rows() const {
				return table->num_rows();
			}
  		std::vector<std::string> names() const;
		
		BlazingTableView toBlazingTableView() const;
  	private:
  		std::vector<std::string> columnNames;
  		std::unique_ptr<CudfTable> table;
  	};


  class BlazingTableView{
  public:
  	BlazingTableView(CudfTableView table,std::vector<std::string> columnNames);
  	CudfTableView view() const;
  	std::vector<std::string> names() const;
	std::unique_ptr<BlazingTable> clone() const;
  private:
  	std::vector<std::string> columnNames;
  	CudfTableView table;
  };

}

}
