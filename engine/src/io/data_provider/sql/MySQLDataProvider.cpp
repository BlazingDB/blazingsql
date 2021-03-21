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

struct mysql_table_info {
  // mysql doest have exact info about partition size ... so for now we dont 
  // need to rely on partition info
  //std::vector<std::string> partitions;
  size_t estimated_table_row_count;
};

struct mysql_columns_info {
  std::vector<std::string> columns;
  std::vector<std::string> types;
  std::vector<size_t> bytes;
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
  // TODO percy set chunk size here
  return ret;
}

std::shared_ptr<sql::ResultSet> execute_mysql_query(sql::Connection *con,
                                                    const std::string &query)
{
  std::unique_ptr<sql::Statement> stmt(con->createStatement());
  std::shared_ptr<sql::ResultSet> res(stmt->executeQuery(query));
  return res;
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
    // TODO percy
  }

  return ret;
}

// TODO percy avoid repeated code
bool is_string_test(const std::string &t) {
  std::vector<std::string> mysql_string_types_hints = {
    "CHARACTER",
    "VARCHAR",
    "VARYING CHARACTER",
    "NCHAR",
    "NATIVE CHARACTER",
    "NVARCHAR",
    "TEXT",
    "CLOB",
    "STRING" // TODO percy ???
  };

  for (auto hint : mysql_string_types_hints) {
    if (StringUtil::beginsWith(t, hint)) return true;
  }

  return false;
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
      std::string col_type = StringUtil::toUpper(res->getString("DATA_TYPE").asStdString());
      size_t max_bytes = 8; // max bytes date = 5+3(frac secs) = 8 ... then the largest comes from strings

      if (is_string_test(col_type)) {
        max_bytes = res->getUInt64("CHARACTER_MAXIMUM_LENGTH");
      }

      ret.columns.push_back(col_name);
      ret.types.push_back(col_type);
      ret.bytes.push_back(max_bytes);
    }
  } catch (sql::SQLException &e) {
    // TODO percy
  }

  return ret;
}

mysql_data_provider::mysql_data_provider(const sql_info &sql)
	: abstractsql_data_provider(sql)
  , mysql_connection(nullptr), estimated_table_row_count(0)
  , batch_position(0), table_fetch_completed(false)
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
  this->column_bytes = cols_info.bytes;
}

mysql_data_provider::~mysql_data_provider() {
}

std::shared_ptr<data_provider> mysql_data_provider::clone() {
  return std::make_shared<mysql_data_provider>(sql);
}

bool mysql_data_provider::has_next() {
  return (this->table_fetch_completed == false);
}

void mysql_data_provider::reset() {
  this->table_fetch_completed = true;
  this->batch_position = 0;
}

data_handle mysql_data_provider::get_next(bool open_file) {
  data_handle ret;

  if (open_file == false) {
    ret.sql_handle.table = this->sql.table;
    ret.sql_handle.column_names = this->column_names;
    ret.sql_handle.column_types = this->column_types;
    return ret;
  }

  std::string select_from = this->build_select_from();
  std::string where = this->sql.table_filter.empty()? "" : " where ";
  std::string query = select_from + where + this->sql.table_filter + this->build_limit_offset(this->batch_position);
  // DEBUG
  //std::cout << "MYSQL QUERY: " << query << "\n";
  this->batch_position += this->sql.table_batch_size;
  auto res = execute_mysql_query(this->mysql_connection.get(), query);

  if (res->rowsCount() == 0) {
    this->table_fetch_completed = true;
  }

  ret.sql_handle.table = this->sql.table;
  ret.sql_handle.column_names = this->column_names;
  ret.sql_handle.column_types = this->column_types;
  ret.sql_handle.column_bytes = this->column_bytes;
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

} /* namespace io */
} /* namespace ral */
