/*
 * Copyright 2021 Percy Camilo Triveño Aucahuasi <percy.camilo.ta@gmail.com>
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
#include "compatibility/SQLTranspiler.h"

#include <mysql/jdbc.h>

using namespace fmt::literals;

namespace ral {
namespace io {

// For mysql operators see https://dev.mysql.com/doc/refman/8.0/en/non-typed-operators.html

struct mysql_table_info {
  // mysql doest have exact info about partition size ... so for now we dont 
  // need to rely on partition info
  //std::vector<std::string> partitions;
  size_t estimated_table_row_count;
};

struct mysql_columns_info {
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
sql::ConnectOptionsMap build_jdbc_mysql_connection(const std::string host,
                                                   const std::string user,
                                                   const std::string password,
                                                   size_t port,
                                                   const std::string schema) {
  sql::ConnectOptionsMap ret; // aka connection properties
  ret["hostName"] = host;
  ret["port"] = (int)port;
  ret["userName"] = user;
  ret["password"] = password;
  ret["schema"] = schema;
  return ret;
}

std::shared_ptr<sql::ResultSet> execute_mysql_query(sql::Connection *con,
                                                    const std::string &query)
{
  auto stmt_deleter = [](sql::Statement *pointer) {
    pointer->close();
    delete pointer;
  };

  auto resultset_deleter = [](sql::ResultSet *pointer) {
    pointer->close();
    delete pointer;
  };

  std::shared_ptr<sql::ResultSet> ret = nullptr;
  try {
    std::shared_ptr<sql::Statement> stmt(con->createStatement(), stmt_deleter);
    ret = std::shared_ptr<sql::ResultSet>(stmt->executeQuery(query), resultset_deleter);
    // NOTE do not enable setFetchSize, we want to read by batches
  } catch (sql::SQLException &e) {
    throw std::runtime_error("ERROR: Could not run the MySQL query  " + query + ": " + e.what());
  }

  return ret;
}

mysql_table_info get_mysql_table_info(sql::Connection *con, const std::string &table) {
  mysql_table_info ret;

  try {
    std::string query = "EXPLAIN PARTITIONS SELECT * FROM " + table;
    auto res = execute_mysql_query(con, query);

    while (res->next()) {
      //std::string parts = res->getString("partitions").asStdString();
      //if (!parts.empty()) {
      //  ret.partitions = StringUtil::split(parts, ',');
      //}
      ret.estimated_table_row_count = res->getInt("rows");
      break; // we should not have more than 1 row here
    }
  } catch (sql::SQLException &e) {
    throw std::runtime_error("ERROR: Could not get table information for MySQL " + table + ": " + e.what());
  }

  return ret;
}

mysql_columns_info get_mysql_columns_info(sql::Connection *con,
                                          const std::string &table) {
  mysql_columns_info ret;

  try {
    std::string db = con->getSchema().asStdString();
    std::string query = "SELECT * from INFORMATION_SCHEMA.COLUMNS WHERE `TABLE_SCHEMA`='"+db+"' AND `TABLE_NAME`='"+table+"'";
    auto res = execute_mysql_query(con, query);

    while (res->next()) {
      std::string col_name = res->getString("COLUMN_NAME").asStdString();
      std::string col_type = StringUtil::toUpper(res->getString("COLUMN_TYPE").asStdString());
      ret.columns.push_back(col_name);
      ret.types.push_back(col_type);
    }
  } catch (sql::SQLException &e) {
    throw std::runtime_error("ERROR: Could not get columns information for MySQL " + table + ": " + e.what());
  }

  return ret;
}

mysql_data_provider::mysql_data_provider(
    const sql_info &sql,
    size_t total_number_of_nodes,
    size_t self_node_idx)
	: abstractsql_data_provider(sql, total_number_of_nodes, self_node_idx)
  , mysql_connection(nullptr), estimated_table_row_count(0)
  , batch_index(0), table_fetch_completed(false)
{
  sql::Driver *driver = sql::mysql::get_driver_instance();
  sql::ConnectOptionsMap options = build_jdbc_mysql_connection(this->sql.host,
                                                               this->sql.user,
                                                               this->sql.password,
                                                               this->sql.port,
                                                               this->sql.schema);
  this->mysql_connection = std::unique_ptr<sql::Connection>(driver->connect(options));
  mysql_table_info tbl_info = get_mysql_table_info(this->mysql_connection.get(),
                                                   this->sql.table);
  this->estimated_table_row_count = tbl_info.estimated_table_row_count;
  mysql_columns_info cols_info = get_mysql_columns_info(this->mysql_connection.get(),
                                                        this->sql.table);
  this->column_names = cols_info.columns;
  this->column_types = cols_info.types;
}

mysql_data_provider::~mysql_data_provider() {
}

std::shared_ptr<data_provider> mysql_data_provider::clone() {
  return std::make_shared<mysql_data_provider>(sql, this->total_number_of_nodes, this->self_node_idx);
}

bool mysql_data_provider::has_next() {
  return (this->table_fetch_completed == false);
}

void mysql_data_provider::reset() {
  this->table_fetch_completed = false;
  this->batch_index = 0;
}

data_handle mysql_data_provider::get_next(bool open_file) {
  data_handle ret;

  if (open_file == false) {
    ret.sql_handle.table = this->sql.table;
    ret.sql_handle.column_names = this->column_names;
    ret.sql_handle.column_types = this->column_types;
    return ret;
  }

  std::string query = this->build_select_query(this->batch_index);

  // DEBUG
  //std::cout << "\n>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>MYSQL QUERY:\n\n" << query << "\n\n\n";
  ++this->batch_index;
  auto res = execute_mysql_query(this->mysql_connection.get(), query);

  if (res->rowsCount() == 0) {
    this->table_fetch_completed = true;
  }

  ret.sql_handle.table = this->sql.table;
  ret.sql_handle.column_names = this->column_names;
  ret.sql_handle.column_types = this->column_types;
  ret.sql_handle.mysql_resultset = res;
  ret.sql_handle.row_count = res->rowsCount();
  // TODO percy add columns to uri.query
  ret.uri = Uri("mysql", "", this->sql.schema + "/" + this->sql.table, "", "");
  return ret;
}

size_t mysql_data_provider::get_num_handles() {
  size_t ret = this->estimated_table_row_count / this->sql.table_batch_size;
  return ret == 0? 1 : ret;
}

std::unique_ptr<ral::parser::node_transformer> mysql_data_provider::get_predicate_transformer() const
{
  return std::unique_ptr<ral::parser::node_transformer>(new sql_tools::default_predicate_transformer(
    this->column_indices,
    this->column_names,
    sql_tools::get_default_operators()
  ));
}

} /* namespace io */
} /* namespace ral */
