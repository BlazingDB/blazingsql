/*
 * Copyright 2021 BlazingDB, Inc.
 *     Copyright 2021 Cristhian Alberto Gonzales Castillo <percy@blazingdb.com>
 */

#include "PostgreSQLDataProvider.h"

#include <sstream>

namespace ral {
namespace io {

const std::string MakePostgreSQLConnectionString(const sql_info &sql) {
  std::ostringstream os;
  os << "dbname = " << sql.schema;
  return os.str();
}

postgresql_data_provider::postgresql_data_provider(const sql_info &sql)
	  : abstractsql_data_provider(sql) {
  connection = PQconnectdb(MakePostgreSQLConnectionString(sql).c_str());

  if (PQstatus(connection) != CONNECTION_OK) {
    std::cerr << "Connection to database failed: "
              << PQerrorMessage(connection)
              << std::endl; // TODO: build error messages by ostreams
    throw std::runtime_error("Connection to database failed: " +
        std::string{PQerrorMessage(connection)});
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
