/**
 * Copyright 2021 Cristhian Alberto Gonzales Castillo <gcca.lib@gmail.com>
 */

#include <sstream>

#include "SnowFlakeDataProvider.h"

namespace ral {
namespace io {

static inline void PopulateTableInfo(SQLHDBC sqlHdbc,
                                     const sql_info & sql,
                                     std::vector<std::string> & column_names,
                                     std::vector<std::string> & column_types,
                                     std::size_t & row_count) {
  SQLRETURN sqlReturn;
  SQLHSTMT sqlHStmt;
  sqlReturn = SQLAllocHandle(SQL_HANDLE_STMT, sqlHdbc, &sqlHStmt);
  if (sqlReturn != SQL_SUCCESS) {
    throw std::runtime_error("SnowFlake: allocation handle statement");
  }

  std::ostringstream oss;
  oss << "select COLUMN_NAME, DATA_TYPE from \"" << sql.schema
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
    std::transform(
        std::cbegin(buffer),
        std::cend(buffer),
        std::begin(buffer),
        [](const std::string::value_type c) { return std::tolower(c); });
    column_types.emplace_back(buffer);

    counter++;
  }
}

snowflake_data_provider::snowflake_data_provider(
    const sql_info & sql,
    std::size_t total_number_of_nodes,
    std::size_t self_node_idx)
    : abstractsql_data_provider{sql, total_number_of_nodes, self_node_idx},
      sqlHEnv{nullptr}, sqlHdbc{nullptr}, row_count{0}, batch_position{0} {
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
  if (sqlReturn != SQL_SUCCESS) {
    throw std::runtime_error("SnowFlake: free hdbc");
  }

  sqlReturn = SQLFreeHandle(SQL_HANDLE_ENV, sqlHEnv);
  if (sqlReturn != SQL_SUCCESS) {
    throw std::runtime_error("SnowFlake: free hdbc");
  }
}

std::shared_ptr<data_provider> snowflake_data_provider::clone() {
  return nullptr;
}

bool snowflake_data_provider::has_next() { return false; }

void snowflake_data_provider::reset() {}

data_handle snowflake_data_provider::get_next(bool) {
  data_handle handle;
  return handle;
}

std::size_t snowflake_data_provider::get_num_handles() { return 0; }

}  // namespace io
}  // namespace ral
