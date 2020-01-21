
#pragma once

#include "cudf/column/column_view.hpp"
#include "cudf/table/table.hpp"
#include "cudf/table/table_view.hpp"
#include <memory>
#include <string>
#include <vector>

typedef cudf::experimental::table CudfTable;
typedef cudf::table_view CudfTableView;
typedef cudf::column_view CudfColumnView;


namespace ral {

namespace frame {

class BlazingTableView;

class BlazingTable {
public:
	BlazingTable(std::unique_ptr<CudfTable> table, std::vector<std::string> columnNames);
	BlazingTable(BlazingTable &&) = default;
	BlazingTable & operator=(BlazingTable const &) = delete;
	BlazingTable & operator=(BlazingTable &&) = delete;

	CudfTableView view() const;
	cudf::size_type num_columns() const { return table->num_columns(); }
	cudf::size_type num_rows() const { return table->num_rows(); }
	std::vector<std::string> names() const;
	// set columnNames
	void setNames(const std::vector<std::string> &names) { this->columnNames = names; }

	BlazingTableView toBlazingTableView() const;

	operator bool() const { return table != nullptr; }

private:
	std::vector<std::string> columnNames;
	std::unique_ptr<CudfTable> table;
};


class BlazingTableView {
public:
	BlazingTableView(CudfTableView table, std::vector<std::string> columnNames);
	BlazingTableView(BlazingTableView const &) = default;
	BlazingTableView(BlazingTableView &&) = default;

	BlazingTableView & operator=(BlazingTableView const &) = default;
	BlazingTableView & operator=(BlazingTableView &&) = default;

	CudfTableView view() const;

	cudf::column_view const & column(cudf::size_type column_index) const { return table.column(column_index); }

	std::vector<std::string> names() const;

	cudf::size_type num_columns() const { return table.num_columns(); }

	cudf::size_type num_rows() const { return table.num_rows(); }

	std::unique_ptr<BlazingTable> clone() const;

private:
	std::vector<std::string> columnNames;
	CudfTableView table;
};

}  // namespace frame

}  // namespace ral

