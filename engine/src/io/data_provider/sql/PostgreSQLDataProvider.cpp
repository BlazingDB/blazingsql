/*
 * Copyright 2021 BlazingDB, Inc.
 *     Copyright 2021 Cristhian Alberto Gonzales Castillo <cristhian@blazingdb.com>
 */

#include <sstream>

#include "PostgreSQLDataProvider.h"

namespace ral {
namespace io {

namespace {

const std::string MakePostgreSQLConnectionString(const sql_info &sql) {
  std::ostringstream os;
  os << "host=" << sql.host
     << " port=" << sql.port
     << " dbname=" << sql.schema
     << " user=" << sql.user
     << " password=" << sql.password;
  return os.str();
}

const std::string MakeQueryForColumnsInfo(const sql_info &sql) {
  std::ostringstream os;
  os << "select column_name, data_type"
        " from information_schema.tables as tables"
        " join information_schema.columns as columns"
        " on tables.table_name = columns.table_name"
        " where tables.table_catalog = '" <<  sql.schema
     << "' and tables.table_name = '" << sql.table <<"'";
  return os.str();
}

class TableInfo {
public:
  std::vector<std::string> column_names;
  std::vector<std::string> column_types;
};

TableInfo ExecuteTableInfo(PGconn *connection, const sql_info &sql) {
  PGresult *result = PQexec(connection, MakeQueryForColumnsInfo(sql).c_str());
  if (PQresultStatus(result) != PGRES_TUPLES_OK) {
    throw std::runtime_error("Error access for columns info");
    PQclear(result);
    PQfinish(connection);
  }

  int resultNfields = PQnfields(result);
  if (resultNfields < 2) {
    throw std::runtime_error("Invalid status for information schema");
  }

  const std::string resultFirstFname{PQfname(result, 0)};
  const std::string resultSecondFname{PQfname(result, 1)};
  if (resultFirstFname != "column_name" || resultSecondFname != "data_type") {
    throw std::runtime_error("Invalid columns for information schema");
  }

  int resultNtuples = PQntuples(result);
  TableInfo tableInfo;
  tableInfo.column_names.reserve(resultNtuples);
  tableInfo.column_types.reserve(resultNtuples);

  for (int i = 0; i < resultNtuples; i++) {
    tableInfo.column_names.emplace_back(std::string{PQgetvalue(result, i, 0)});
    tableInfo.column_types.emplace_back(std::string{PQgetvalue(result, i, 1)});
  }

  return tableInfo;
}

}

postgresql_data_provider::postgresql_data_provider(const sql_info &sql)
	  : abstractsql_data_provider(sql), table_fetch_completed{false},
      batch_position{0}, estimated_table_row_count{0} {
  connection = PQconnectdb(MakePostgreSQLConnectionString(sql).c_str());

  if (PQstatus(connection) != CONNECTION_OK) {
    std::cerr << "Connection to database failed: "
              << PQerrorMessage(connection)
              << std::endl; // TODO: build error messages by ostreams
    throw std::runtime_error("Connection to database failed: " +
        std::string{PQerrorMessage(connection)});
  }

  std::cout << "PostgreSQL version: "
            << PQserverVersion(connection) << std::endl;

  TableInfo tableInfo = ExecuteTableInfo(connection, sql);

  column_names = tableInfo.column_names;
  column_types = tableInfo.column_types;
}

postgresql_data_provider::~postgresql_data_provider() {
  PQfinish(connection);
}

std::shared_ptr<data_provider> postgresql_data_provider::clone() {
  return std::static_pointer_cast<data_provider>(
      std::make_shared<postgresql_data_provider>(sql));
}

bool postgresql_data_provider::has_next() {
  return table_fetch_completed == false;
}

void postgresql_data_provider::reset() {
  table_fetch_completed = true;
  batch_position = 0;
}

data_handle postgresql_data_provider::get_next(bool) {
  return data_handle{};
}

std::size_t postgresql_data_provider::get_num_handles() {
  std::size_t ret = estimated_table_row_count / sql.table_batch_size;
  return ret == 0 ? ret : 1;
}

} /* namespace io */
} /* namespace ral */
