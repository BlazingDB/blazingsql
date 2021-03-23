/*
 * Copyright 2021 BlazingDB, Inc.
 *     Copyright 2021 Cristhian Alberto Gonzales Castillo <percy@blazingdb.com>
 */

#include "PostgreSQLDataProvider.h"

namespace ral {
namespace io {

postgresql_data_provider::postgresql_data_provider(const sql_connection &sql_conn,
                                                   const std::string &table,
                                                   size_t batch_size_hint,
                                                   bool use_partitions)
	  : abstractsql_data_provider(sql_conn, table, batch_size_hint, use_partitions) {
  std::string dbname = "dbtest";
  auto connection = PQconnectdb(("dbname = " + dbname).c_str());


  if (PQstatus(connection) != CONNECTION_OK) {
    throw std::runtime_error("invalid postgresql connection");
  }
}

postgresql_data_provider::~postgresql_data_provider() = default;

} /* namespace io */
} /* namespace ral */
