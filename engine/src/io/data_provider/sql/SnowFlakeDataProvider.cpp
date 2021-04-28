/**
 * Copyright 2021 Cristhian Alberto Gonzales Castillo <gcca.lib@gmail.com>
 */

#include <sstream>

#include "SnowFlakeDataProvider.h"

// trick to avoid compilation errores from arrow data type bool
// TODO: change abstractsql_data_provider hierarchy to follow the private
// implementation patterns to fix the coupling and linking issues
#define BOOL int
#include <sql.h>
#include <sqlext.h>

namespace ral {
namespace io {

static inline void
ReadRowCount(SQLHDBC sqlHdbc, const sql_info & sql, std::size_t & row_count) {
  SQLHSTMT sqlHStmt;
  SQLRETURN sqlReturn = SQLAllocHandle(SQL_HANDLE_STMT, sqlHdbc, &sqlHStmt);
  if (sqlReturn != SQL_SUCCESS) {
    throw std::runtime_error(
        "SnowFlake: allocation handle statement for row counting");
  }

  const std::string query = "select count(*) from " + sql.table;
  SQLCHAR * statementText =
      reinterpret_cast<SQLCHAR *>(const_cast<char *>(query.c_str()));
  sqlReturn = SQLExecDirect(sqlHStmt, statementText, SQL_NTS);
  if (sqlReturn != SQL_SUCCESS) {
    throw std::runtime_error("SnowFlake: exec direct for row counting");
  }

  if (SQL_SUCCEEDED(sqlReturn = SQLFetch(sqlHStmt))) {
    SQLLEN indicator;
    sqlReturn = SQLGetData(sqlHStmt,
                           1,
                           SQL_C_LONG,
                           static_cast<SQLPOINTER>(&row_count),
                           static_cast<SQLLEN>(sizeof(row_count)),
                           &indicator);
    if (SQL_SUCCEEDED(sqlReturn)) {
      if (indicator == SQL_NULL_DATA) {
        throw std::runtime_error(
            "SnowFlake: null returned instead of row count");
      }
    }
  } else {
    throw std::runtime_error("SnowFlake: reading result for row counting");
  }

  sqlReturn = SQLFreeHandle(SQL_HANDLE_STMT, sqlHStmt);
  if (sqlReturn != SQL_SUCCESS) {
    throw std::runtime_error("SnowFlake: free stmt for row count");
  }
}

static inline std::string MapColumnType(const char buffer[],
                                        const std::size_t scale) {
  const std::string dataType{buffer};
  if (dataType == "text") { return "string"; }
  // maybe it could be use precision to check size tiny, small, big
  if (dataType == "number") { return scale ? "int64" : "float64"; }
  if (dataType == "float") { return "float64"; }
  if (dataType == "date") { return "date"; }
  if (dataType == "boolean") { return "boolean"; }
  if (dataType == "timestamp_ltz") { return "timestamp"; }
  if (dataType == "variant") { return "string"; }  // TODO: struct

  std::ostringstream oss;
  oss << "SnowFlake: unsupported type " << buffer;
  throw std::runtime_error{oss.str()};
}

static inline void PopulateTableInfo(SQLHDBC sqlHdbc,
                                     const sql_info & sql,
                                     std::vector<std::string> & column_names,
                                     std::vector<std::string> & column_types,
                                     std::size_t & row_count) {
  SQLRETURN sqlReturn;
  SQLHSTMT sqlHStmt;
  sqlReturn = SQLAllocHandle(SQL_HANDLE_STMT, sqlHdbc, &sqlHStmt);
  if (sqlReturn != SQL_SUCCESS) {
    throw std::runtime_error(
        "SnowFlake: allocation handle statement for information schema");
  }

  std::ostringstream oss;
  oss << "select COLUMN_NAME, DATA_TYPE, NUMERIC_SCALE from \"" << sql.schema
      << "\".\"INFORMATION_SCHEMA\".\"COLUMNS\" where TABLE_SCHEMA = '"
      << sql.sub_schema << "' and TABLE_NAME = '" << sql.table << "';";
  const std::string query = oss.str();
  SQLCHAR * statementText =
      reinterpret_cast<SQLCHAR *>(const_cast<char *>(query.c_str()));
  sqlReturn = SQLExecDirect(sqlHStmt, statementText, SQL_NTS);
  if (sqlReturn != SQL_SUCCESS) {
    throw std::runtime_error("SnowFlake: exec direct for information schema");
  }

  SQLSMALLINT num_columns;
  sqlReturn = SQLNumResultCols(sqlHStmt, &num_columns);

  SQLLEN counter = 0;
  while (SQL_SUCCEEDED(sqlReturn = SQLFetch(sqlHStmt))) {
    SQLLEN indicator;
    char buffer[256];

    // column name
    sqlReturn = SQLGetData(sqlHStmt,
                           1,
                           SQL_C_CHAR,
                           static_cast<SQLPOINTER>(buffer),
                           static_cast<SQLLEN>(sizeof(buffer)),
                           &indicator);
    if (SQL_SUCCEEDED(sqlReturn)) {
      if (indicator == SQL_NULL_DATA) {
        std::ostringstream oss;
        oss << "SnowFlake: internal error getting column name "
            << " Row count: " << counter;
        throw std::runtime_error(oss.str());
      }
    }
    column_names.emplace_back(buffer);

    // data type
    sqlReturn = SQLGetData(sqlHStmt,
                           2,
                           SQL_C_CHAR,
                           static_cast<SQLPOINTER>(buffer),
                           static_cast<SQLLEN>(sizeof(buffer)),
                           &indicator);
    if (SQL_SUCCEEDED(sqlReturn)) {
      if (indicator == SQL_NULL_DATA) {
        std::ostringstream oss;
        oss << "SnowFlake: internal error getting column name "
            << " Row count: " << counter;
        throw std::runtime_error(oss.str());
      }
    }
    std::transform(
        std::cbegin(buffer),
        std::cend(buffer),
        std::begin(buffer),
        [](const std::string::value_type c) { return std::tolower(c); });

    // scale
    std::size_t scale;
    sqlReturn = SQLGetData(sqlHStmt,
                           3,
                           SQL_C_LONG,
                           static_cast<SQLPOINTER>(&scale),
                           static_cast<SQLLEN>(sizeof(scale)),
                           &indicator);
    if (!SQL_SUCCEEDED(sqlReturn)) {
      throw std::runtime_error("SnowFlake: getting scale");
    }

    column_types.emplace_back(MapColumnType(buffer, scale));

    counter++;
  }

  sqlReturn = SQLFreeHandle(SQL_HANDLE_STMT, sqlHStmt);
  if (sqlReturn != SQL_SUCCESS) {
    throw std::runtime_error("SnowFlake: free stmt for table info");
  }

  ReadRowCount(sqlHdbc, sql, row_count);
}

snowflake_data_provider::snowflake_data_provider(
    const sql_info & sql,
    std::size_t total_number_of_nodes,
    std::size_t self_node_idx)
    : abstractsql_data_provider{sql, total_number_of_nodes, self_node_idx},
      sqlHEnv{nullptr}, sqlHdbc{nullptr}, row_count{0},
      batch_position{0}, completed{false} {
  SQLRETURN sqlReturn = SQL_ERROR;
  sqlReturn = SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &sqlHEnv);
  if (sqlReturn != SQL_SUCCESS) {
    throw std::runtime_error("SnowFlake: Allocation handle environment");
  }

  sqlReturn = SQLSetEnvAttr(sqlHEnv,
                            SQL_ATTR_ODBC_VERSION,
                            reinterpret_cast<SQLPOINTER>(SQL_OV_ODBC3),
                            static_cast<SQLINTEGER>(0));
  if (sqlReturn != SQL_SUCCESS) {
    throw std::runtime_error("SnowFlake: Setting environment attribute");
  }

  sqlReturn = SQLAllocHandle(SQL_HANDLE_DBC, sqlHEnv, &sqlHdbc);
  if (sqlReturn != SQL_SUCCESS) {
    throw std::runtime_error("SnowFlake: Allocation handle database");
  }

  std::ostringstream oss;
  oss << "Dsn=" << sql.dsn << ";Database=" << sql.schema
      << ";Schema=" << sql.sub_schema << ";uid=" << sql.user
      << ";pwd=" << sql.password;
  ;
  std::string connectionStdString = oss.str();
  SQLCHAR * connectionString = reinterpret_cast<SQLCHAR *>(
      const_cast<char *>(connectionStdString.c_str()));
  sqlReturn = SQLDriverConnect(sqlHdbc,
                               nullptr,
                               connectionString,
                               SQL_NTS,
                               nullptr,
                               0,
                               nullptr,
                               SQL_DRIVER_COMPLETE);
  if (sqlReturn != SQL_SUCCESS) {
    throw std::runtime_error("SnowFlake: driver connection");
  }

  PopulateTableInfo(sqlHdbc, sql, column_names, column_types, row_count);
}

snowflake_data_provider::~snowflake_data_provider() {
  SQLRETURN sqlReturn = SQL_ERROR;

  sqlReturn = SQLFreeHandle(SQL_HANDLE_DBC, sqlHdbc);
  //if (sqlReturn != SQL_SUCCESS) {
    //throw std::runtime_error("SnowFlake: free hdbc");
  //}

  sqlReturn = SQLFreeHandle(SQL_HANDLE_ENV, sqlHEnv);
  //if (sqlReturn != SQL_SUCCESS) {
    //throw std::runtime_error("SnowFlake: free env");
  //}
}

std::shared_ptr<data_provider> snowflake_data_provider::clone() {
  return std::static_pointer_cast<data_provider>(
      std::make_shared<snowflake_data_provider>(
          sql, total_number_of_nodes, self_node_idx));
}

bool snowflake_data_provider::has_next() { return !completed; }

void snowflake_data_provider::reset() { batch_position = 0; }

data_handle snowflake_data_provider::get_next(bool open_file) {
  data_handle handle;

  handle.sql_handle.table = sql.table;
  handle.sql_handle.column_names = column_names;
  handle.sql_handle.column_types = column_types;

  if (open_file == false) { return handle; }

  std::ostringstream oss;
  oss << build_select_query(batch_position);
  const std::string query = oss.str();
  batch_position++;

  SQLHSTMT sqlHStmt;
  SQLRETURN sqlReturn = SQLAllocHandle(SQL_HANDLE_STMT, sqlHdbc, &sqlHStmt);
  if (sqlReturn != SQL_SUCCESS) {
    throw std::runtime_error(
        "SnowFlake: allocation handle statement for information schema");
  }
  SQLCHAR * statementText =
      reinterpret_cast<SQLCHAR *>(const_cast<char *>(query.c_str()));
  sqlReturn = SQLExecDirect(sqlHStmt, statementText, SQL_NTS);
  if (sqlReturn != SQL_SUCCESS) {
    throw std::runtime_error("SnowFlake: exec direct getting next batch");
  }

  auto sqlHStmt_deleter = [](SQLHSTMT * sqlHStmt) {
    if (SQLFreeHandle(SQL_HANDLE_STMT, *sqlHStmt) != SQL_SUCCESS) {
      //throw std::runtime_error("SnowFlake: free statement for get next batch");
    }
    delete sqlHStmt;
  };
  handle.sql_handle.snowflake_sqlhdbc.reset(new SQLHSTMT{sqlHStmt},
                                            sqlHStmt_deleter);

  SQLLEN RowCount = 0;
  sqlReturn = SQLRowCount(sqlHStmt, &RowCount);
  if (sqlReturn != SQL_SUCCESS) {
    throw std::runtime_error("SnowFlake: getting row count for query: " +
                             query);
  }
  handle.sql_handle.row_count = static_cast<std::size_t>(RowCount);
  completed = !RowCount;

  handle.uri = Uri("snowflake", "", sql.schema + "/" + sql.table, "", "");

  return handle;
}

std::size_t snowflake_data_provider::get_num_handles() {
  std::size_t ret = row_count / sql.table_batch_size;
  return ret == 0 ? ret : 1;
}

}  // namespace io
}  // namespace ral
