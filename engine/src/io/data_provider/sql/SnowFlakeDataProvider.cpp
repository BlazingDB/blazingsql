/**
 * Copyright 2021 Cristhian Alberto Gonzales Castillo <gcca.lib@gmail.com>
 */

#include <sstream>

#include "SnowFlakeDataProvider.h"

namespace ral {
namespace io {

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
