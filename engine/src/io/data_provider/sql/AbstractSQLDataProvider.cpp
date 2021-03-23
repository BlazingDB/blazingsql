/*
 * Copyright 2021 BlazingDB, Inc.
 *     Copyright 2021 Percy Camilo Triveño Aucahuasi <percy@blazingdb.com>
 */

#include "AbstractSQLDataProvider.h"

namespace ral {
namespace io {

abstractsql_data_provider::abstractsql_data_provider(const sql_info &sql)
	: data_provider(), sql(sql) {}

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

std::string abstractsql_data_provider::build_select_from() const {
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
  return "SELECT " + cols + "FROM " + this->sql.table;
}

std::string abstractsql_data_provider::build_limit_offset(size_t offset) const {
  return " LIMIT " + std::to_string(this->sql.table_batch_size) + " OFFSET " + std::to_string(offset);
}

} /* namespace io */
} /* namespace ral */
