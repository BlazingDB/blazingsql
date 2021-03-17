/*
 * Copyright 2021 BlazingDB, Inc.
 *     Copyright 2021 Percy Camilo Triveño Aucahuasi <percy@blazingdb.com>
 */

#include "AbstractSQLDataProvider.h"

namespace ral {
namespace io {

abstractsql_data_provider::abstractsql_data_provider(
        const sql_connection &sql_conn,
        const std::string &table,
        size_t batch_size_hint,
        bool use_partitions)
	: data_provider(), sql_conn(sql_conn), table(table)
  , batch_size_hint(batch_size_hint), use_partitions(use_partitions)
  , batch_position(0), row_count(0), partition_count(0), current_row_count(0) {}

abstractsql_data_provider::~abstractsql_data_provider() {
	this->close_file_handles(); 
}

bool abstractsql_data_provider::has_next() {
  return this->current_row_count < row_count;
}

void abstractsql_data_provider::reset() {
	this->batch_position = 0;
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

} /* namespace io */
} /* namespace ral */
