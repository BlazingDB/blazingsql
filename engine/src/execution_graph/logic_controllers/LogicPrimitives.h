
#pragma once

#include "execution_graph/logic_controllers/BlazingColumn.h"
#include "execution_graph/logic_controllers/BlazingColumnOwner.h"
#include "execution_graph/logic_controllers/BlazingColumnView.h"


namespace ral {

namespace frame {

class BlazingTable;
class BlazingTableView;

class BlazingTable {
public:
	BlazingTable(std::unique_ptr<CudfTable> table, std::vector<std::string> columnNames);
	BlazingTable(BlazingTable &&) = default;
	BlazingTable & operator=(BlazingTable const &) = delete;
	BlazingTable & operator=(BlazingTable &&) = delete;

	CudfTableView view() const;
	cudf::size_type num_columns() const { return columns.size(); }
	cudf::size_type num_rows() const { return columns.size() == 0 ? 0 : columns[0]->view().size(); }
	std::vector<std::string> names() const;
	// set columnNames
	void setNames(const std::vector<std::string> &names) { this->columnNames = names; }

	BlazingTableView toBlazingTableView() const;

	operator bool() const { return columns.size() != 0; }

	std::unique_ptr<CudfTable> releaseCudfTable();

private:
	std::vector<std::string> columnNames;
	std::vector<std::unique_ptr<BlazingColumn>> columns;
};


class BlazingTableView {
public:
	BlazingTableView();
	BlazingTableView(CudfTableView table, std::vector<std::string> columnNames);
	BlazingTableView(BlazingTableView const &) = default;
	BlazingTableView(BlazingTableView &&) = default;

	BlazingTableView & operator=(BlazingTableView const &) = default;
	BlazingTableView & operator=(BlazingTableView &&) = default;

	CudfTableView view() const;

	cudf::column_view const & column(cudf::size_type column_index) const { return table.column(column_index); }

	std::vector<std::string> names() const;
	// set columnNames
	void setNames(const std::vector<std::string> &names) { this->columnNames = names; }

	cudf::size_type num_columns() const { return table.num_columns(); }

	cudf::size_type num_rows() const { return table.num_rows(); }

	std::unique_ptr<BlazingTable> clone() const;

private:
	std::vector<std::string> columnNames;
	CudfTableView table;
};

typedef std::pair<std::unique_ptr<ral::frame::BlazingTable>, ral::frame::BlazingTableView> TableViewPair;

TableViewPair createEmptyTableViewPair(std::vector<cudf::type_id> column_types,
									   std::vector<std::string> column_names);

}  // namespace frame

}  // namespace ral

