/*
 * Copyright 2021 BlazingDB, Inc.
 *     Copyright 2021 Percy Camilo Triveño Aucahuasi <percy@blazingdb.com>
 */

// NOTES 
/*
  The JDBC API throws three different exceptions:

- sql::MethodNotImplementedException (derived from sql::SQLException)
- sql::InvalidArgumentException (derived from sql::SQLException)
- sql::SQLException (derived from std::runtime_error)
*/

#include "MySQLDataProvider.h"
#include "blazingdb/io/Util/StringUtil.h"

#include <mysql/jdbc.h>

using namespace fmt::literals;

namespace ral {
namespace io {

struct table_info {
  std::vector<std::string> partitions;
  size_t rows;
};

struct columns_info {
  std::vector<std::string> columns;
  std::vector<std::string> types;
};

/* MySQL supports these connection properties:
  - hostName
  - userName
  - password
  - port
  - socket
  - pipe
  - characterSetResults
  - schema
  - sslKey
  - sslCert
  - sslCA
  - sslCAPath
  - sslCipher
  - defaultStatementResultType
  - defaultPreparedStatementResultType
  - CLIENT_COMPRESS
  - CLIENT_FOUND_ROWS
  - CLIENT_IGNORE_SIGPIPE
  - CLIENT_IGNORE_SPACE
  - CLIENT_INTERACTIVE
  - CLIENT_LOCAL_FILES
  - CLIENT_MULTI_RESULTS
  - CLIENT_MULTI_STATEMENTS
  - CLIENT_NO_SCHEMA
  - CLIENT_COMPRESS
  - OPT_CONNECT_TIMEOUT
  - OPT_NAMED_PIPE
  - OPT_READ_TIMEOUT
  - OPT_WRITE_TIMEOUT
  - OPT_RECONNECT
  - OPT_CHARSET_NAME
  - OPT_REPORT_DATA_TRUNCATION
*/
sql::ConnectOptionsMap build_connection_properties(const sql_connection &sql_conn) {
  sql::ConnectOptionsMap ret; // aka connection properties
  ret["hostName"] = sql_conn.host;
  ret["userName"] = sql_conn.user;
  ret["password"] = sql_conn.password;
  ret["schema"] = sql_conn.schema;
  // TODO percy set chunk size here
  return ret;
}

std::shared_ptr<sql::ResultSet> execute_query(sql::Connection *con,
                                              const std::string &query)
{
  std::unique_ptr<sql::Statement> stmt(con->createStatement());
  std::shared_ptr<sql::ResultSet> res(stmt->executeQuery(query));
  return res;
}

table_info get_table_info(sql::Connection *con, const std::string &table) {
  table_info ret;

  try {
    std::string query = "EXPLAIN PARTITIONS SELECT * FROM " + table;
    auto res = execute_query(con, query);

    while (res->next()) {
      std::string parts = res->getString("partitions").asStdString();
      if (!parts.empty()) {
        ret.partitions = StringUtil::split(parts, ',');
      }
      ret.rows = res->getInt("rows");
      break; // we should not have more than 1 row here
    }
  } catch (sql::SQLException &e) {
    // TODO percy
  }

  return ret;
}

columns_info get_columns_info(sql::Connection *con,
                              const std::string &table) {
  columns_info ret;

  try {
    std::string db = con->getSchema().asStdString();
    std::string query = "SELECT * from INFORMATION_SCHEMA.COLUMNS WHERE `TABLE_SCHEMA`='"+db+"' AND `TABLE_NAME`='"+table+"'";
    auto res = execute_query(con, query);

    while (res->next()) {
      std::string col_name = res->getString("COLUMN_NAME").asStdString();
      std::string col_type = StringUtil::toUpper(res->getString("DATA_TYPE").asStdString());
      ret.columns.push_back(col_name);
      ret.types.push_back(col_type);
    }
  } catch (sql::SQLException &e) {
    // TODO percy
  }

  return ret;
}

mysql_data_provider::mysql_data_provider(const sql_connection &sql_conn,
                                         const std::string &table,
                                         size_t batch_size_hint,
                                         bool use_partitions)
	: abstractsql_data_provider(sql_conn, table, batch_size_hint, use_partitions)
  , mysql_connection(nullptr) {
  sql::Driver *driver = sql::mysql::get_driver_instance();
  sql::ConnectOptionsMap options = build_connection_properties(this->sql_conn);
  this->mysql_connection = std::unique_ptr<sql::Connection>(driver->connect(options));
  table_info tbl_info = get_table_info(this->mysql_connection.get(), this->table);
  this->partitions = std::move(tbl_info.partitions);
  this->row_count = tbl_info.rows;
  columns_info cols_info = get_columns_info(this->mysql_connection.get(), this->table);
  this->columns = cols_info.columns;
  this->types = cols_info.types;
}

mysql_data_provider::~mysql_data_provider() {
}

std::shared_ptr<data_provider> mysql_data_provider::clone() {
  return std::make_shared<mysql_data_provider>(this->sql_conn, this->table, this->batch_size_hint);
}

size_t mysql_data_provider::get_num_handles() {
  if (this->partitions.empty()) {
    size_t ret = this->row_count / this->batch_size_hint;
    return ret == 0? 1 : ret;
  }

  return this->partitions.size();
}

data_handle mysql_data_provider::get_next(bool) {
  std::string query;

  if (this->use_partitions) {
    // TODO percy if part size less than batch full part fetch else apply limit offset over the partition to fetch
    query = "SELECT * FROM " + this->table + " partition(" + this->partitions[this->batch_position++] + ")";
  } else {
    query = "SELECT * FROM " + this->table + " LIMIT " + std::to_string(this->batch_size_hint) + " OFFSET " + std::to_string(this->batch_position);
    this->batch_position += this->batch_size_hint;
  }

  std::cout << "query: " << query << "\n";
  auto res = execute_query(this->mysql_connection.get(), query);
  this->current_row_count += res->rowsCount();
  data_handle ret;
  ret.sql_handle.table = this->table;
  ret.sql_handle.column_names = this->columns;
  ret.sql_handle.column_types = this->types;
  ret.sql_handle.mysql_resultset = res;
  std::cout << "get_next TOTAL rows: " << this->row_count << "\n";
  std::cout << "get_next current_row_count: " << this->current_row_count << "\n";
  return ret;
}

} /* namespace io */
} /* namespace ral */
