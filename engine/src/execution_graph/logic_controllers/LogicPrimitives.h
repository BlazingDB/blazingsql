
#pragma once

#include "cudf/column/column_view.hpp"
#include "cudf/table/table.hpp"
#include "cudf/table/table_view.hpp"
#include <cudf/io/functions.hpp>
#include <future>
#include <memory>
#include <mutex>
#include <queue>
#include <string>
#include <typeindex>
#include <vector>
#include <string>
#include "execution_graph/logic_controllers/BlazingColumn.h"
#include "execution_graph/logic_controllers/BlazingColumnOwner.h"
#include "execution_graph/logic_controllers/BlazingColumnView.h"
#include <blazingdb/transport/ColumnTransport.h>
#include <bmr/BlazingMemoryResource.h>

typedef cudf::experimental::table CudfTable;
typedef cudf::table_view CudfTableView;
typedef cudf::column CudfColumn;
typedef cudf::column_view CudfColumnView;
namespace cudf_io = cudf::experimental::io;

namespace ral {
namespace frame {

class BlazingTable;
class BlazingTableView;
class BlazingHostTable;

class BlazingTable {
public:
	BlazingTable(std::vector<std::unique_ptr<BlazingColumn>> columns, const std::vector<std::string> & columnNames);
	BlazingTable(std::unique_ptr<CudfTable> table, const std::vector<std::string> & columnNames);
	BlazingTable(const CudfTableView & table, const std::vector<std::string> & columnNames);
	BlazingTable(BlazingTable &&) = default;
	BlazingTable & operator=(BlazingTable const &) = delete;
	BlazingTable & operator=(BlazingTable &&) = delete;

	CudfTableView view() const;
	cudf::size_type num_columns() const { return columns.size(); }
	cudf::size_type num_rows() const { return columns.size() == 0 ? 0 : columns[0]->view().size(); }
	std::vector<std::string> names() const;
	std::vector<cudf::data_type> get_schema() const;
	// set columnNames
	void setNames(const std::vector<std::string> & names) { this->columnNames = names; }

	BlazingTableView toBlazingTableView() const;

	operator bool() const { return columns.size() != 0; }

	std::unique_ptr<CudfTable> releaseCudfTable();
	std::vector<std::unique_ptr<BlazingColumn>> releaseBlazingColumns();

	unsigned long long sizeInBytes();

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
	std::vector<std::unique_ptr<BlazingColumn>> toBlazingColumns() const;

	std::vector<cudf::data_type> get_schema();

	std::vector<std::string> names() const;
	void setNames(const std::vector<std::string> & names) { this->columnNames = names; }

	cudf::size_type num_columns() const { return table.num_columns(); }

	cudf::size_type num_rows() const { return table.num_rows(); }

	std::unique_ptr<BlazingTable> clone() const;

private:
	std::vector<std::string> columnNames;
	CudfTableView table;
};
using ColumnTransport = blazingdb::transport::experimental::ColumnTransport;

class BlazingHostTable {
public:
	BlazingHostTable(const std::vector<ColumnTransport> &columns_offsets, std::vector<std::basic_string<char>> &&raw_buffers)
		: columns_offsets{columns_offsets}, raw_buffers{std::move(raw_buffers)} 
	{
		auto size = sizeInBytes();
		blazing_host_memory_mesource::getInstance().allocate(size);
	}
	~BlazingHostTable() {
		auto size = sizeInBytes();
		blazing_host_memory_mesource::getInstance().deallocate(size);
	}

	std::vector<cudf::data_type> get_schema() const {
		std::vector<cudf::data_type> data_types(this->num_columns());
		std::transform(columns_offsets.begin(), columns_offsets.end(), data_types.begin(), [](auto & col){ 
			int32_t dtype = col.metadata.dtype;
			return cudf::data_type{cudf::type_id(dtype)}; 
		});
		return data_types;
	}

	std::vector<std::string> names() const {
		std::vector<std::string> col_names(this->num_columns());
		std::transform(columns_offsets.begin(), columns_offsets.end(), col_names.begin(), [](auto & col){ return col.metadata.col_name; });
		return col_names;
	}

	cudf::size_type num_rows() const { 
		return columns_offsets.empty() ? 0 : columns_offsets.front().metadata.size; 
	}

	cudf::size_type num_columns() const {
		return columns_offsets.size(); 
	}

	unsigned long long sizeInBytes() {
		unsigned long long total_size = 0L;
		for (auto &col : columns_offsets) {
			total_size += col.size_in_bytes;
		}
		return total_size;
	}

	void setPartitionId(const size_t &part_id) {
		this->part_id = part_id;
	}

	size_t get_part_id() {
		return this->part_id;
	}

	const std::vector<ColumnTransport> & get_columns_offsets() const {
		return columns_offsets;
	}

	const std::vector<std::basic_string<char>> & get_raw_buffers() const{
		return raw_buffers;
	}

private:
	std::vector<ColumnTransport> columns_offsets;
	std::vector<std::basic_string<char>> raw_buffers;
	size_t part_id;
};

std::unique_ptr<ral::frame::BlazingTable> createEmptyBlazingTable(std::vector<cudf::type_id> column_types,
									   std::vector<std::string> column_names);

std::vector<std::unique_ptr<BlazingColumn>> cudfTableViewToBlazingColumns(const CudfTableView & table);

}  // namespace frame
}  // namespace ral
