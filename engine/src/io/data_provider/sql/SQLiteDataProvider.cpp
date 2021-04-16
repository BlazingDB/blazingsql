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

#include <sstream>

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
};

struct callb {
  int sqlite_callback(void *, int argc, char ** argv, char ** azColName) {
    int i;
    for (i = 0; i < argc; i++) {
      printf("%s = %s\n", azColName[i], argv[i] ? argv[i] : "NULL");
    }
    printf("\n");
    return 0;
  }
};

static inline std::shared_ptr<sqlite3_stmt>
execute_sqlite_query(sqlite3 * db, const std::string & query) {
  sqlite3_stmt * stmt;

  int errorCode = sqlite3_prepare_v2(db, query.c_str(), -1, &stmt, nullptr);
  if (errorCode != SQLITE_OK) {
    std::ostringstream oss;
    oss << "Executing SQLite query provider: " << std::endl
        << "query: " << query << std::endl
        << "error message: " << sqlite3_errmsg(db);
    throw std::runtime_error{oss.str()};
  }

  auto sqlite_deleter = [](sqlite3_stmt * stmt) { sqlite3_finalize(stmt); };
  return std::shared_ptr<sqlite3_stmt>{stmt, sqlite_deleter};
}

static inline sqlite_table_info
get_sqlite_table_info(sqlite3 * db, const std::string & table) {
  sqlite_table_info ret;
  const std::string sql{"select count(*) from " + table};
  int err = sqlite3_exec(
      db,
      sql.c_str(),
      [](void * data, int count, char ** rows, char **) -> int {
        if (count == 1 && rows) {
          sqlite_table_info & ret = *static_cast<sqlite_table_info *>(data);
          ret.partitions.push_back("default");  // check for partitions api
          ret.rows = static_cast<std::size_t>(atoi(rows[0]));
          return 0;
        }
        return 1;
      },
      &ret,
      nullptr);
  if (err != SQLITE_OK) { throw std::runtime_error("getting number of rows"); }
  return ret;
}

// TODO percy avoid code duplication
bool sqlite_is_string_col_type(const std::string & t) {
  std::vector<std::string> mysql_string_types_hints = {
      "character",
      "varchar",
      "char",
      "varying character",
      "nchar",
      "native character",
      "nvarchar",
      "text",
      "clob",
      "string"  // TODO percy ???
  };

  for (auto hint : mysql_string_types_hints) {
    if (StringUtil::beginsWith(t, hint)) return true;
  }

  return false;
}

static inline sqlite_columns_info
get_sqlite_columns_info(sqlite3 * db, const std::string & table) {
  sqlite_columns_info ret;
  std::string query = "PRAGMA table_info(" + table + ")";
  auto A = execute_sqlite_query(db, query);
  sqlite3_stmt * stmt = A.get();

  int rc = SQLITE_ERROR;
  while ((rc = sqlite3_step(stmt)) == SQLITE_ROW) {
    const unsigned char * name = sqlite3_column_text(stmt, 1);
    std::string col_name(reinterpret_cast<const char *>(name));
    ret.columns.push_back(col_name);

    const unsigned char * type = sqlite3_column_text(stmt, 2);
    std::string col_type(reinterpret_cast<const char *>(type));

    std::transform(
        col_type.cbegin(),
        col_type.cend(),
        col_type.begin(),
        [](const std::string::value_type c) { return std::tolower(c); });

    std::size_t max_bytes = 8;  // TODO percy check max scalar bytes from sqlite
    if (sqlite_is_string_col_type(col_type)) {
      // max_bytes = res->getUInt64("CHARACTER_MAXIMUM_LENGTH");
      // TODO percy see how to get the max size for string/txt cols ...
      // see docs
      max_bytes = 256;
    }
    ret.types.push_back(col_type);
  }

  if (rc != SQLITE_DONE) {
    std::ostringstream oss;
    oss << "Getting SQLite columns info: " << std::endl
        << "query: " << query << std::endl
        << "error message: " << sqlite3_errmsg(db);
    throw std::runtime_error{oss.str()};
  }

  return ret;
}

sqlite_data_provider::sqlite_data_provider(const sql_info & sql,
                                           size_t total_number_of_nodes,
                                           size_t self_node_idx)
    : abstractsql_data_provider{sql, total_number_of_nodes, self_node_idx},
      db{nullptr}, current_stmt{nullptr}, batch_position{0}, current_row_count{
                                                                 0} {
  int errorCode = sqlite3_open(sql.schema.c_str(), &db);

  if (errorCode != SQLITE_OK) {
    throw std::runtime_error(std::string{"Can't open database: "} +
                             sqlite3_errmsg(db));
  }

  sqlite_table_info tbl_info = get_sqlite_table_info(db, sql.table);
  partitions = std::move(tbl_info.partitions);
  row_count = tbl_info.rows;

  sqlite_columns_info cols_info = get_sqlite_columns_info(db, sql.table);
  column_names = cols_info.columns;
  column_types = cols_info.types;
}

sqlite_data_provider::~sqlite_data_provider() { sqlite3_close(db); }

std::shared_ptr<data_provider> sqlite_data_provider::clone() {
  return std::make_shared<sqlite_data_provider>(sql,
                                                total_number_of_nodes,
                                                self_node_idx);
}

bool sqlite_data_provider::has_next() { return current_stmt != nullptr; }

void sqlite_data_provider::reset() { batch_position = 0; }

data_handle sqlite_data_provider::get_next(bool) {
  data_handle ret;

  const std::int32_t limit = batch_position + sql.table_batch_size;
  const std::int32_t offset = batch_position;

  std::ostringstream oss;
  oss << "SELECT * FROM " << sql.table << " LIMIT " << limit << " OFFSET "
      << offset;
  std::string query = oss.str();

  batch_position += sql.table_batch_size;

  std::shared_ptr<sqlite3_stmt> stmt = execute_sqlite_query(db, query);

  current_row_count += batch_position;

  ret.sql_handle.table = sql.table;
  ret.sql_handle.column_names = column_names;
  ret.sql_handle.column_types = column_types;
  ret.sql_handle.row_count = row_count;
  ret.sql_handle.sqlite_statement = stmt;

  // TODO percy add columns to uri.query
  ret.uri = Uri("sqlite", "", sql.schema + "/" + sql.table, "", "");
  return ret;
}

size_t sqlite_data_provider::get_num_handles() {
  if (partitions.empty()) {
    size_t ret = row_count / sql.table_batch_size;
    return ret == 0 ? 1 : ret;
  }
  return partitions.size();
}

} /* namespace io */
} /* namespace ral */
