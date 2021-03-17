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
namespace mysql {

struct table_info {
  std::vector<std::string> partitions;
  size_t rows;
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

std::shared_ptr<sql::ResultSet> execute_query(sql::Connection *con, const std::string &query) {
  std::unique_ptr<sql::Statement> stmt(con->createStatement());
  std::shared_ptr<sql::ResultSet> res(stmt->executeQuery(query));
  return res;
}

table_info get_table_info(sql::Connection *con, std::string table) {
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

mysql_data_provider::mysql_data_provider(const sql_connection &sql_conn,
                                         const std::string &table,
                                         size_t batch_size_hint)
	: abstractsql_data_provider(sql_conn, table, batch_size_hint)
  , mysql_connection(nullptr)
  , rows(0) {
  sql::Driver *driver = sql::mysql::get_driver_instance();
  sql::ConnectOptionsMap options = build_connection_properties(this->sql_conn);
  this->mysql_connection = std::unique_ptr<sql::Connection>(driver->connect(options));
  table_info tbl_info = get_table_info(this->mysql_connection.get(), this->table);
  this->partitions = std::move(tbl_info.partitions);
  this->rows = tbl_info.rows;
}

mysql_data_provider::~mysql_data_provider() {
}

std::shared_ptr<data_provider> mysql_data_provider::clone() {
  return std::make_shared<mysql_data_provider>(this->sql_conn, this->table, this->batch_size_hint);
}

size_t mysql_data_provider::get_num_handles() {
  if (this->partitions.empty()) {
    size_t ret = this->rows / this->batch_size_hint;
    return ret == 0? 1 : ret;
  }

  return this->partitions.size();
}

bool mysql_data_provider::has_next() {
  if (this->rows == 0) {
    return false;
  }

  if (this->batch_position == 0) {
    return true;
  }
  
  if (this->partitions.empty()) {
    return this->batch_position < (this->rows + this->batch_size_hint);
  }

  return this->batch_position < this->partitions.size();
}

data_handle mysql_data_provider::get_next(bool open_file) {
  std::string query;

  if (this->partitions.empty()) {
    query = "SELECT * FROM " + this->table + " LIMIT " + std::to_string(this->batch_size_hint) + " OFFSET " + std::to_string(this->batch_position);
    this->batch_position += this->batch_size_hint;
    
  } else {
    query = "SELECT * FROM " + this->table + " partition(" + this->partitions[this->batch_position++] + ")";
  }

  std::cout << "query: " << query << "\n";
  auto res = execute_query(this->mysql_connection.get(), query);
  data_handle ret;
  ret.mysql_resultset = res;
  return ret;
}

} /* namespace mysql */
} /* namespace io */
} /* namespace ral */
