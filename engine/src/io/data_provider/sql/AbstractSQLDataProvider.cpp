/*
 * Copyright 2021 Percy Camilo Triveño Aucahuasi <percy.camilo.ta@gmail.com>
 */

#include "AbstractSQLDataProvider.h"

#include "compatibility/SQLTranspiler.h"

namespace ral {
namespace io {

abstractsql_data_provider::abstractsql_data_provider(
    const sql_info &sql,
    size_t total_number_of_nodes,
    size_t self_node_idx)
	: data_provider(), sql(sql), total_number_of_nodes(total_number_of_nodes), self_node_idx(self_node_idx) {}

abstractsql_data_provider::~abstractsql_data_provider() {
	this->close_file_handles();
}

std::vector<data_handle> abstractsql_data_provider::get_some(std::size_t batch_count, bool){
	std::size_t count = 0;
	std::vector<data_handle> file_handles;
	while(this->has_next() && count < batch_count) {
		auto handle = this->get_next();
		if (handle.is_valid()) {
			file_handles.emplace_back(std::move(handle));
		}
		count++;
	}
	return file_handles;
}

/**
 * Closes currently open set of file handles maintained by the provider
*/
void abstractsql_data_provider::close_file_handles() {
  // NOTE we don't use any file handle for this provider so nothing to do here
}

bool abstractsql_data_provider::set_predicate_pushdown(const std::string &queryString)
{
  // DEBUG
  //std::cout << "\nORIGINAL query part for the predicate pushdown:\n" << queryString << "\n\n";
  auto predicate_transformer = this->get_predicate_transformer();
  this->where = sql_tools::transpile_predicate(queryString, predicate_transformer.get());
  // DEBUG
  //std::cout << "\nWHERE stmt for the predicate pushdown:\n" << this->where << "\n\n";
  return !this->where.empty();
}

std::string abstractsql_data_provider::build_select_query(size_t batch_index) const {
  std::string cols;

  if (this->column_indices.empty()) {
    cols = "* ";
  } else {
    for (int i = 0; i < this->column_indices.size(); ++i) {
      int col_idx = this->column_indices[i];
      cols += this->column_names[col_idx];
      if ((i + 1) == this->column_indices.size()) {
        cols += " ";
      } else {
        cols += ", ";
      }
    }
  }

  const size_t offset = this->sql.table_batch_size * (this->total_number_of_nodes * batch_index + this->self_node_idx);
  std::string limit = " LIMIT " + std::to_string(this->sql.table_batch_size) + " OFFSET " + std::to_string(offset);
  auto ret = "SELECT " + cols + "FROM " + this->sql.table;

  if(!sql.table_filter.empty() && !this->where.empty()) {
    ret += " where " + sql.table_filter + " AND " + this->where;
  } else {
    if (sql.table_filter.empty()) {
      if (!this->where.empty()) { // then the filter is from the predicate pushdown{
        ret += " where " + this->where;
      }
    } else {
      ret += " where " + sql.table_filter;
    }
  }

  return ret + limit;
}

} /* namespace io */
} /* namespace ral */
