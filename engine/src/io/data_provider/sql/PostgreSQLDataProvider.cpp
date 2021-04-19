/*
 * Copyright 2021 BlazingDB, Inc.
 *     Copyright 2021 Cristhian Alberto Gonzales Castillo
 * <cristhian@blazingdb.com>
 */

#include <sstream>

#include <netinet/in.h>

#include "PostgreSQLDataProvider.h"

namespace ral {
namespace io {

namespace {

const std::string MakePostgreSQLConnectionString(const sql_info & sql) {
  std::ostringstream os;
  os << "host=" << sql.host << " port=" << sql.port << " dbname=" << sql.schema
     << " user=" << sql.user << " password=" << sql.password;
  return os.str();
}

const std::string MakeQueryForColumnsInfo(const sql_info & sql) {
  std::ostringstream os;
  os << "select column_name, data_type, character_maximum_length"
        " from information_schema.tables as tables"
        " join information_schema.columns as columns"
        " on tables.table_name = columns.table_name"
        " where tables.table_catalog = '"
     << sql.schema << "' and tables.table_name = '" << sql.table << "'";
  return os.str();
}

class TableInfo {
public:
  std::vector<std::string> column_names;
  std::vector<std::string> column_types;
  std::vector<std::size_t> column_bytes;
  std::size_t row_count;
};

inline TableInfo ExecuteTableInfo(PGconn * connection, const sql_info & sql) {
  PGresult * result = PQexec(connection, MakeQueryForColumnsInfo(sql).c_str());
  if (PQresultStatus(result) != PGRES_TUPLES_OK) {
    PQclear(result);
    PQfinish(connection);
    throw std::runtime_error("Error access for columns info");
  }

  int resultNtuples = PQntuples(result);
  int resultNfields = PQnfields(result);
  TableInfo tableInfo;
  tableInfo.column_names.reserve(resultNfields);
  tableInfo.column_types.reserve(resultNfields);
  tableInfo.row_count = static_cast<std::size_t>(resultNtuples);

  int columnNameFn = PQfnumber(result, "column_name");
  int dataTypeFn = PQfnumber(result, "data_type");
  int characterMaximumLengthFn = PQfnumber(result, "character_maximum_length");

  for (int i = 0; i < resultNfields; i++) {
    tableInfo.column_names.emplace_back(
        std::string{PQgetvalue(result, i, columnNameFn)});
    tableInfo.column_types.emplace_back(
        std::string{PQgetvalue(result, i, dataTypeFn)});

    // NOTE character_maximum_length is used for char or byte string type
    if (PQgetisnull(result, i, characterMaximumLengthFn)) {
      // TODO(recy, cristhian): check the minimum size for types
      tableInfo.column_bytes.emplace_back(8);
    } else {
      const char * characterMaximumLengthBytes =
          PQgetvalue(result, i, characterMaximumLengthFn);
      // NOTE postgresql representation of number is in network order
      const std::uint32_t characterMaximumLength =
          ntohl(*reinterpret_cast<const std::uint32_t *>(
              characterMaximumLengthBytes));
      tableInfo.column_bytes.emplace_back(
          static_cast<const std::size_t>(characterMaximumLength));
    }
  }

  return tableInfo;
}

}  // namespace

postgresql_data_provider::postgresql_data_provider(
    const sql_info & sql,
    std::size_t total_number_of_nodes,
    std::size_t self_node_idx)
    : abstractsql_data_provider(sql, total_number_of_nodes, self_node_idx),
      table_fetch_completed{false}, batch_position{0},
      estimated_table_row_count{0} {
  connection = PQconnectdb(MakePostgreSQLConnectionString(sql).c_str());

  if (PQstatus(connection) != CONNECTION_OK) {
    throw std::runtime_error("Connection to database failed: " +
                             std::string{PQerrorMessage(connection)});
  }

  TableInfo tableInfo = ExecuteTableInfo(connection, sql);
  column_names = tableInfo.column_names;
  column_types = tableInfo.column_types;
  column_bytes = tableInfo.column_bytes;
  estimated_table_row_count = tableInfo.row_count;
}

postgresql_data_provider::~postgresql_data_provider() { PQfinish(connection); }

std::shared_ptr<data_provider> postgresql_data_provider::clone() {
  return std::static_pointer_cast<data_provider>(
      std::make_shared<postgresql_data_provider>(sql,
                                                 this->total_number_of_nodes,
                                                 this->self_node_idx));
}

bool postgresql_data_provider::has_next() {
  return this->table_fetch_completed == false;
}

void postgresql_data_provider::reset() {
  this->table_fetch_completed = false;
  this->batch_position = 0;
}

data_handle postgresql_data_provider::get_next(bool open_file) {
  data_handle handle;

  handle.sql_handle.table = sql.table;
  handle.sql_handle.column_names = column_names;
  handle.sql_handle.column_types = column_types;

  if (!open_file) { return handle; }

  const std::string select_from = build_select_from();
  const std::string where = sql.table_filter.empty() ? "" : " where ";
  const std::size_t offset =
      sql.table_batch_size *
      (batch_position * total_number_of_nodes + self_node_idx);
  const std::string query =
      select_from + where + sql.table_filter + build_limit_offset(offset);

  batch_position++;
  PGresult * result = PQexecParams(connection,
                                   query.c_str(),
                                   0,
                                   nullptr,
                                   nullptr,
                                   nullptr,
                                   nullptr,
                                   1);

  if (PQresultStatus(result) != PGRES_TUPLES_OK) {
    PQclear(result);
    PQfinish(connection);
    throw std::runtime_error("Error getting next batch from postgresql");
  }

  int resultNtuples = PQntuples(result);

  if (!resultNtuples) { table_fetch_completed = true; }

  handle.sql_handle.column_bytes = column_bytes;
  handle.sql_handle.postgresql_result.reset(result, PQclear);
  handle.sql_handle.row_count = PQntuples(result);
  handle.uri = Uri("postgresql", "", sql.schema + "/" + sql.table, "", "");

  return handle;
}

std::size_t postgresql_data_provider::get_num_handles() {
  std::size_t ret = estimated_table_row_count / sql.table_batch_size;
  return ret == 0 ? ret : 1;
}

} /* namespace io */
} /* namespace ral */
