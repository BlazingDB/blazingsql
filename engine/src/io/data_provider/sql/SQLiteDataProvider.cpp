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

#include "SQLiteDataProvider.h"
#include "blazingdb/io/Util/StringUtil.h"

using namespace fmt::literals;

namespace ral {
namespace io {

struct sqlite_table_info {
  std::vector<std::string> partitions;
  size_t rows;
};

struct sqlite_columns_info {
  std::vector<std::string> columns;
  std::vector<std::string> types;
};

static int sqlite_callback(void *NotUsed, int argc, char **argv, char **azColName) {
   int i;
   for(i = 0; i<argc; i++) {
      printf("%s = %s\n", azColName[i], argv[i] ? argv[i] : "NULL");
   }
   printf("\n");
   return 0;
}

std::vector<std::vector<std::string>> execute_sqlite_query(sqlite3 *conn,
                                                     const std::string &query)
{
  std::vector<std::vector<std::string>> res;
  char *zErrMsg = 0;
  int rc = sqlite3_exec(conn, query.c_str(), sqlite_callback, 0, &zErrMsg);
  
  if (rc != SQLITE_OK){
     fprintf(stderr, "SQL error: %s\n", zErrMsg);
     sqlite3_free(zErrMsg);
  } else {
     fprintf(stdout, "run sqlite query successfully\n");
  }

  //std::unique_ptr<sql::Statement> stmt(con->createStatement());
  //std::shared_ptr<sql::ResultSet> res(stmt->executeQuery(query));
  return res;
}

sqlite_table_info get_sqlite_table_info(sqlite3 *conn, const std::string &table)
{
  // TODO percy error handling

  sqlite_table_info ret;

//  std::string query = "EXPLAIN PARTITIONS SELECT * FROM " + table;
//  auto res = execute_sqlite_query(conn, query);

//  while (res->next()) {
//    std::string parts = res->getString("partitions").asStdString();
//    if (!parts.empty()) {
//      ret.partitions = StringUtil::split(parts, ',');
//    }
//    ret.rows = res->getInt("rows");
//    break; // we should not have more than 1 row here
//  }

  return ret;
}

sqlite_columns_info get_sqlite_columns_info(sqlite3 *conn,
                                            const std::string &table)
{
  // TODO percy error handling
  
  sqlite_columns_info ret;

//    std::string db = con->getSchema().asStdString();
//    std::string query = "SELECT * from INFORMATION_SCHEMA.COLUMNS WHERE `TABLE_SCHEMA`='"+db+"' AND `TABLE_NAME`='"+table+"'";
//    auto res = execute_sqlite_query(con, query);

//    while (res->next()) {
//      std::string col_name = res->getString("COLUMN_NAME").asStdString();
//      std::string col_type = StringUtil::toUpper(res->getString("DATA_TYPE").asStdString());
//      ret.columns.push_back(col_name);
//      ret.types.push_back(col_type);
//    }


  return ret;
}

sqlite_data_provider::sqlite_data_provider(const sql_connection &sql_conn,
                                           const std::string &table,
                                           size_t batch_size_hint,
                                           bool use_partitions)
	: abstractsql_data_provider(sql_conn, table, batch_size_hint, use_partitions)
  , sqlite_connection(nullptr)
{
  sqlite3 *conn = nullptr;
  int rc = sqlite3_open(sql_conn.schema.c_str(), &conn);

  if( rc ) {
     fprintf(stderr, "Can't open database: %s\n", sqlite3_errmsg(conn));
     // TODO percy error
  } else {
     fprintf(stdout, "Opened sqlite database successfully\n");
  }

  this->sqlite_connection = conn;
  sqlite_table_info tbl_info = get_sqlite_table_info(conn, this->table);
  this->partitions = std::move(tbl_info.partitions);
  this->row_count = tbl_info.rows;
  sqlite_columns_info cols_info = get_sqlite_columns_info(conn, this->table);
  this->columns = cols_info.columns;
  this->types = cols_info.types;
}

sqlite_data_provider::~sqlite_data_provider() {
  sqlite3_close(this->sqlite_connection);
}

std::shared_ptr<data_provider> sqlite_data_provider::clone() {
  return std::make_shared<sqlite_data_provider>(this->sql_conn, this->table, this->batch_size_hint);
}

size_t sqlite_data_provider::get_num_handles() {
  if (this->partitions.empty()) {
    size_t ret = this->row_count / this->batch_size_hint;
    return ret == 0? 1 : ret;
  }

  return this->partitions.size();
}

data_handle sqlite_data_provider::get_next(bool) {
  std::string query;

//  if (this->use_partitions) {
//    // TODO percy if part size less than batch full part fetch else apply limit offset over the partition to fetch
//    query = "SELECT * FROM " + this->table + " partition(" + this->partitions[this->batch_position++] + ")";
//  } else {
//    query = "SELECT * FROM " + this->table + " LIMIT " + std::to_string(this->batch_size_hint) + " OFFSET " + std::to_string(this->batch_position);
//    this->batch_position += this->batch_size_hint;
//  }

//  std::cout << "query: " << query << "\n";
//  auto res = execute_sqlite_query(this->sqlite_connection.get(), query);
//  this->current_row_count += res->rowsCount();
  data_handle ret;
  ret.sql_handle.table = this->table;
//  ret.sql_handle.column_names = this->columns;
//  ret.sql_handle.column_types = this->types;
//  ret.sql_handle.sqlite_resultset = res;
  // TODO percy add columns to uri.query
  ret.uri = Uri("mysql", "", this->sql_conn.schema + "/" + this->table, "", "");
//  std::cout << "get_next TOTAL rows: " << this->row_count << "\n";
//  std::cout << "get_next current_row_count: " << this->current_row_count << "\n";
//  return ret;
}

} /* namespace io */
} /* namespace ral */
