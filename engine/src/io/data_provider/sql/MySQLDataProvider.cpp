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
};

struct mysql_operator_info {
  std::string label;
  ral::parser::operator_node::placement_type placement = ral::parser::operator_node::AUTO;
  bool parentheses_wrap = true;
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

static std::map<operator_type, mysql_operator_info> get_mysql_operators() {
  using ral::parser::operator_node;
  
  // see https://dev.mysql.com/doc/refman/8.0/en/non-typed-operators.html
  static std::map<operator_type, mysql_operator_info> operators;
  if (operators.empty()) {
    operators[operator_type::BLZ_IS_NOT_NULL] = {.label = "IS NOT NULL", .placement = operator_node::END};
    operators[operator_type::BLZ_IS_NULL] = {.label = "IS NULL", .placement = operator_node::END};
  }
  return operators;
}

class mysql_predicate_transformer : public parser::node_transformer {
public:
  mysql_predicate_transformer(
    const std::vector<int> &column_indices,
    const std::vector<std::string> &column_names)
    : column_indices(column_indices), column_names(column_names) {}

  virtual ~mysql_predicate_transformer() {}

  parser::node * transform(parser::operad_node& node) override {
    auto ndir = &((ral::parser::node&)node);
    if (this->visited.count(ndir)) {
      return &node;
    }
    if (node.type == ral::parser::node_type::VARIABLE) {
      std::string var = StringUtil::split(node.value, "$")[1];
      size_t idx = std::atoi(var.c_str());
      size_t col = column_indices[idx];
      node.value = column_names[col];
    } else if (node.type == ral::parser::node_type::LITERAL) {
      ral::parser::literal_node &literal_node = ((ral::parser::literal_node&)node);
      if (literal_node.type().id() == cudf::type_id::TIMESTAMP_DAYS ||
          literal_node.type().id() == cudf::type_id::TIMESTAMP_SECONDS ||
          literal_node.type().id() == cudf::type_id::TIMESTAMP_NANOSECONDS ||
          literal_node.type().id() == cudf::type_id::TIMESTAMP_MICROSECONDS ||
          literal_node.type().id() == cudf::type_id::TIMESTAMP_MILLISECONDS)
      {
        node.value = "\"" + node.value + "\"";
      }
    }
    this->visited[ndir] = true;
    return &node;
  }

  parser::node * transform(parser::operator_node& node) override {
    auto ndir = &((ral::parser::node&)node);
    if (this->visited.count(ndir)) {
      return &node;
    }
    if (!get_mysql_operators().empty()) {
      operator_type op = map_to_operator_type(node.value);
      if (get_mysql_operators().count(op)) {
        auto op_obj = get_mysql_operators().at(op);
        node.label = op_obj.label;
        node.placement = op_obj.placement;
        node.parentheses_wrap = op_obj.parentheses_wrap;
      }
    }
    this->visited[ndir] = true;
    return &node;
  }

private:
  std::map<ral::parser::node*, bool> visited;
  std::vector<int> column_indices;
  std::vector<std::string> column_names;
};

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
  return std::unique_ptr<ral::parser::node_transformer>(new mysql_predicate_transformer(
    this->column_indices,
    this->column_names
  ));
}

} /* namespace io */
} /* namespace ral */
