/*
 * Copyright 2021 Percy Camilo Triveño Aucahuasi <percy.camilo.ta@gmail.com>
 * Copyright 2021 Cristhian Alberto Gonzales Castillo
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
  TableInfo tableInfo;
  tableInfo.column_names.reserve(resultNtuples);
  tableInfo.column_types.reserve(resultNtuples);

  int columnNameFn = PQfnumber(result, "column_name");
  int dataTypeFn = PQfnumber(result, "data_type");
  int characterMaximumLengthFn = PQfnumber(result, "character_maximum_length");

  for (int i = 0; i < resultNtuples; i++) {
    tableInfo.column_names.emplace_back(
        std::string{PQgetvalue(result, i, columnNameFn)});
    tableInfo.column_types.emplace_back(
        std::string{PQgetvalue(result, i, dataTypeFn)});

    // NOTE character_maximum_length is used for char or byte string type
    if (!PQgetisnull(result, i, characterMaximumLengthFn)) {
      const char * characterMaximumLengthBytes =
          PQgetvalue(result, i, characterMaximumLengthFn);
      // NOTE postgresql representation of number is in network order
      const std::uint32_t characterMaximumLength =
          ntohl(*reinterpret_cast<const std::uint32_t *>(
              characterMaximumLengthBytes));
    }
  }
  PQclear(result);
  const std::string query = "select count(*) from " + sql.table;
  result = PQexec(connection, query.c_str());
  if (PQresultStatus(result) != PGRES_TUPLES_OK) {
    PQclear(result);
    PQfinish(connection);
    throw std::runtime_error("Error access for columns info");
  }
  const char * value = PQgetvalue(result, 0, 0);
  char * end;
  tableInfo.row_count = std::strtoll(value, &end, 10);
  PQclear(result);
  return tableInfo;
}

}  // namespace

static inline std::string FindKeyName(PGconn * connection,
                                      const sql_info & sql) {
  // This function exists because when we get batches from table we use LIMIT
  // clause and since postgresql returns unpredictable subsets of query's rows,
  // we apply a group by a column in order to keep some order for result query
  // see https://www.postgresql.org/docs/13/queries-limit.html
  std::ostringstream oss;
  oss << "select column_name, ordinal_position"
         " from information_schema.table_constraints tc"
         " join information_schema.key_column_usage kcu"
         " on tc.constraint_name = kcu.constraint_name"
         " and tc.constraint_schema = kcu.constraint_schema"
         " and tc.constraint_name = kcu.constraint_name"
         " where tc.table_catalog = '"
      << sql.schema << "' and tc.table_name = '" << sql.table
      << "' and constraint_type = 'PRIMARY KEY'";
  const std::string query = oss.str();
  PGresult * result = PQexec(connection, query.c_str());
  if (PQresultStatus(result) != PGRES_TUPLES_OK) {
    PQclear(result);
    PQfinish(connection);
    throw std::runtime_error("Error access for columns info");
  }

  if (PQntuples(result)) {
    int columnNameFn = PQfnumber(result, "column_name");
    const std::string columnName{PQgetvalue(result, 0, columnNameFn)};
    PQclear(result);
    if (columnName.empty()) {
      throw std::runtime_error("No column name into result for primary key");
    } else {
      return columnName;
    }
  } else {
    // here table doesn't have a primary key, so we choose a column by type
    // the primitive types like int or float have priority over other types
    PQclear(result);
    std::ostringstream oss;
    oss << "select column_name, oid, case"
           " when typname like 'int_' then 1"
           " when typname like 'float_' then 2"
           " else 99 end as typorder"
           " from information_schema.tables as tables"
           " join information_schema.columns as columns"
           " on tables.table_name = columns.table_name"
           " join pg_type on udt_name = typname where tables.table_catalog = '"
        << sql.schema << "' and tables.table_name = '" << sql.table
        << "' order by typorder, typlen desc, oid";

    const std::string query = oss.str();
    PGresult * result = PQexec(connection, query.c_str());
    if (PQresultStatus(result) != PGRES_TUPLES_OK) {
      PQclear(result);
      PQfinish(connection);
      throw std::runtime_error("Error access for columns info");
    }

    if (PQntuples(result)) {
      int columnNameFn = PQfnumber(result, "column_name");
      const std::string columnName{PQgetvalue(result, 0, columnNameFn)};
      PQclear(result);
      if (columnName.empty()) {
        throw std::runtime_error("No column name into result for column type");
      } else {
        return columnName;
      }
    }
    PQclear(result);
  }
  throw std::runtime_error("There is no a key name candidate");
}

static inline bool IsThereNext(PGconn * connection, const std::string & query) {
  std::ostringstream oss;
  oss << "select count(*) from (" << query << ") as t";
  const std::string count = oss.str();
  PGresult * result = PQexec(connection, count.c_str());
  if (PQresultStatus(result) != PGRES_TUPLES_OK) {
    PQclear(result);
    PQfinish(connection);
    throw std::runtime_error("Count query batch");
  }

  const char * data = PQgetvalue(result, 0, 0);
  char * end;
  const std::size_t value =
      static_cast<std::size_t>(std::strtoll(data, &end, 10));
  PQclear(result);

  return value == 0;
}

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
  estimated_table_row_count = tableInfo.row_count;
  keyname = FindKeyName(connection, sql);
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

  if (open_file == false) { return handle; }

  std::ostringstream oss;
  oss << build_select_query(batch_position, keyname);
  const std::string query = oss.str();
  batch_position++;

  PGresult * result = PQexec(connection, query.c_str());
  if (PQresultStatus(result) != PGRES_TUPLES_OK) {
    PQclear(result);
    PQfinish(connection);
    throw std::runtime_error("Error getting next batch from postgresql");
  }
  PQflush(connection);

  int resultNtuples = PQntuples(result);
  {
    std::ostringstream oss;
    oss << "\033[32mQUERY: " << query << std::endl
        << "COUNT: " << resultNtuples << "\033[0m" << std::endl;
    std::cout << oss.str();
  }

  if (!resultNtuples ||
      IsThereNext(connection, build_select_query(batch_position, keyname))) {
    table_fetch_completed = true;
  }

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
