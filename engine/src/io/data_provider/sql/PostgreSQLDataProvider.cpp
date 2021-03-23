/*
 * Copyright 2021 BlazingDB, Inc.
 *     Copyright 2021 Cristhian Alberto Gonzales Castillo <percy@blazingdb.com>
 */

#include "PostgreSQLDataProvider.h"

namespace ral {
namespace io {

postgresql_data_provider::postgresql_data_provider(const sql_info &sql)
	  : abstractsql_data_provider(sql) {
  std::string dbname = "dbtest";
  connection = PQconnectdb(("dbname = " + dbname).c_str());

  if (PQstatus(connection) != CONNECTION_OK) {
    throw std::runtime_error("invalid postgresql connection");
  }
}

postgresql_data_provider::~postgresql_data_provider() {
  PQfinish(connection);
}

std::shared_ptr<data_provider> postgresql_data_provider::clone() {
  return nullptr;
}

bool postgresql_data_provider::has_next() {
  return false;
}

void postgresql_data_provider::reset() {}

data_handle postgresql_data_provider::get_next(bool) {
  return data_handle{};
}

std::size_t postgresql_data_provider::get_num_handles() {
  return 0;
}

} /* namespace io */
} /* namespace ral */
