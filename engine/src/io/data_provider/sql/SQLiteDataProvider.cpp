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

#include "SQLiteDataProvider.h"
#include "blazingdb/io/Util/StringUtil.h"

using namespace fmt::literals;

namespace ral {
namespace io {

struct sqlite_table_info {
  std::vector<std::string> partitions;
  std::size_t rows;
};

struct sqlite_columns_info {
  std::vector<std::string> columns;
  std::vector<std::string> types;
  std::vector<size_t> bytes;
};

struct callb {
  int sqlite_callback(
      void * NotUsed, int argc, char ** argv, char ** azColName) {
    int i;
    for(i = 0; i < argc; i++) {
      printf("%s = %s\n", azColName[i], argv[i] ? argv[i] : "NULL");
    }
    printf("\n");
    return 0;
  }
};

std::shared_ptr<sqlite3_stmt> execute_sqlite_query(
    sqlite3 * conn, const std::string & query) {
  sqlite3_stmt * stmt;
  const char * sql = query.c_str();
  int rc = sqlite3_prepare_v2(conn, sql, -1, &stmt, NULL);
  if(rc != SQLITE_OK) {
    printf("error: %s", sqlite3_errmsg(conn));
    // TODO percy error
  }
  auto sqlite_deleter = [](sqlite3_stmt * pointer) {
    std::cout << "sqlite smt deleted!!!!\n";
    sqlite3_finalize(pointer);
  };
  std::shared_ptr<sqlite3_stmt> ret(stmt, sqlite_deleter);
  return ret;
}

sqlite_table_info get_sqlite_table_info(
    sqlite3 * db, const std::string & table) {
  sqlite_table_info ret;
  const std::string sql{"select count(*) from " + table};
  int err = sqlite3_exec(
      db,
      sql.c_str(),
      [](void * data, int count, char ** rows, char **) -> int {
        if(count == 1 && rows) {
          sqlite_table_info & ret = *static_cast<sqlite_table_info *>(data);
          ret.partitions.push_back("default");  // check for partitions api
          ret.rows = static_cast<std::size_t>(atoi(rows[0]));
          return 0;
        }
        return 1;
      },
      &ret,
      nullptr);
  if(err != SQLITE_OK) { throw std::runtime_error("getting number of rows"); }
  return ret;
}

// TODO percy avoid code duplication
bool sqlite_is_string_col_type(const std::string & t) {
  std::vector<std::string> mysql_string_types_hints = {
      "character",
      "varchar",
      "varying character",
      "nchar",
      "native character",
      "nvarchar",
      "text",
      "clob",
      "string"  // TODO percy ???
  };

  for(auto hint : mysql_string_types_hints) {
    if(StringUtil::beginsWith(t, hint)) return true;
  }

  return false;
}

sqlite_columns_info get_sqlite_columns_info(
    sqlite3 * conn, const std::string & table) {
  // TODO percy error handling

  sqlite_columns_info ret;
  std::string query = "PRAGMA table_info(" + table + ")";
  auto A = execute_sqlite_query(conn, query);
  sqlite3_stmt * stmt = A.get();

  int rc = 0;
  while((rc = sqlite3_step(stmt)) == SQLITE_ROW) {
    const unsigned char * name = sqlite3_column_text(stmt, 1);
    std::string col_name((char *) name);
    ret.columns.push_back(col_name);

    const unsigned char * type = sqlite3_column_text(stmt, 2);
    std::string col_type((char *) type);

    std::transform(col_type.cbegin(),
        col_type.cend(),
        col_type.begin(),
        [](const std::string::value_type c) { return std::tolower(c); });

    size_t max_bytes = 8;  // TODO percy check max scalar bytes from sqlite
    if(sqlite_is_string_col_type(col_type)) {
      // max_bytes = res->getUInt64("CHARACTER_MAXIMUM_LENGTH");
      // TODO percy see how to get the max size for string/txt cols ...
      // see docs
      max_bytes = 256;
    }
    ret.types.push_back(col_type);
  }
  if(rc != SQLITE_DONE) {
    printf("error: %s", sqlite3_errmsg(conn));
    // TODO percy error
  }

  return ret;
}

sqlite_data_provider::sqlite_data_provider(
    const sql_info & sql, size_t total_number_of_nodes, size_t self_node_idx)
    : abstractsql_data_provider(sql, total_number_of_nodes, self_node_idx),
      sqlite_connection(nullptr), batch_position(0), current_row_count(0) {
  sqlite3 * conn = nullptr;
  int rc = sqlite3_open(sql.schema.c_str(), &conn);

  if(rc) {
    fprintf(stderr, "Can't open database: %s\n", sqlite3_errmsg(conn));
    // TODO percy error
  } else {
    fprintf(stdout, "Opened sqlite database successfully\n");
  }

  this->sqlite_connection = conn;
  sqlite_table_info tbl_info = get_sqlite_table_info(conn, this->sql.table);
  this->partitions = std::move(tbl_info.partitions);
  this->row_count = tbl_info.rows;
  sqlite_columns_info cols_info =
      get_sqlite_columns_info(conn, this->sql.table);
  this->column_names = cols_info.columns;
  this->column_types = cols_info.types;
}

sqlite_data_provider::~sqlite_data_provider() {
  sqlite3_close(this->sqlite_connection);
}

std::shared_ptr<data_provider> sqlite_data_provider::clone() {
  return std::make_shared<sqlite_data_provider>(
      this->sql, this->total_number_of_nodes, this->self_node_idx);
}

bool sqlite_data_provider::has_next() {
  return this->current_row_count < row_count;
}

void sqlite_data_provider::reset() { this->batch_position = 0; }

data_handle sqlite_data_provider::get_next(bool) {
  std::string query = this->build_select_query(this->batch_position);
  this->batch_position += this->sql.table_batch_size;

  std::cout << "query: " << query << "\n";
  auto stmt = execute_sqlite_query(this->sqlite_connection, query);
  current_row_count += batch_position;
  data_handle ret;
  ret.sql_handle.table = this->sql.table;
  ret.sql_handle.column_names = this->column_names;
  ret.sql_handle.column_types = this->column_types;
  ret.sql_handle.row_count = row_count;
  ret.sql_handle.sqlite_statement = stmt;
  // TODO percy add columns to uri.query
  ret.uri = Uri("sqlite", "", this->sql.schema + "/" + this->sql.table, "", "");
  //  std::cout << "get_next TOTAL rows: " << this->row_count << "\n";
  //  std::cout << "get_next current_row_count: " << this->current_row_count
  //  << "\n";
  return ret;
}

size_t sqlite_data_provider::get_num_handles() {
  if(this->partitions.empty()) {
    size_t ret = this->row_count / this->sql.table_batch_size;
    return ret == 0 ? 1 : ret;
  }

  return this->partitions.size();
}

} /* namespace io */
} /* namespace ral */
