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

} /* namespace io */
} /* namespace ral */
