#include "tests/utilities/BlazingUnitTest.h"
#include "io/data_provider/sql/MySQLDataProvider.h"
#include "io/data_provider/sql/SQLiteDataProvider.h"
#include "io/data_provider/sql/PostgreSQLDataProvider.h"
#include "io/data_parser/sql/MySQLParser.h"
#include "io/data_parser/sql/SQLiteParser.h"
#include "io/data_parser/sql/PostgreSQLParser.h"
#include <cudf_test/column_wrapper.hpp>
#include <cudf_test/column_utilities.hpp>
#include "utilities/DebuggingUtils.h"
#include <cudf_test/column_utilities.hpp>

#include <sqlite3.h>

struct SQLProviderTest : public BlazingUnitTest {};

TEST_F(SQLProviderTest, DISABLED_postgresql_select_all) {
  ral::io::sql_info sql;
  sql.host = "localhost";
  sql.port = 5432;
  sql.user = "myadmin";
  sql.password = "";
  sql.schema = "pagila";
  sql.table = "actor";
  sql.table_filter = "";
  sql.table_batch_size = 2000;

  auto postgresql_provider =
      std::make_shared<ral::io::postgresql_data_provider>(sql);

  ral::io::postgresql_parser parser;
  ral::io::Schema schema;
  // false so we make sure dont go to the  db and get the schema info only
  auto handle = postgresql_provider->get_next(false);

  parser.parse_schema(handle, schema);
}

TEST_F(SQLProviderTest, mysql_select_all) {
	ral::io::sql_info sql;
  sql.host = "localhost";
  sql.port = 3306;
//  sql.user = "blazing";
//  sql.password = "admin";
//  sql.schema = "bz3";
//  //sql.table = "departments";
  //sql.table = "DATABASECHANGELOG";
  sql.table = "new_table";
  //sql.table = "blazing_catalog_column_datatypes";
//  sql.table_filter = "";
//  sql.table_batch_size = 100;

  sql.user = "lucho";
  sql.password = "admin";
  sql.schema = "employees";
  //sql.table = "departments";
  sql.table = "employees";
  //sql.table = "dept_manager";

  
  sql.schema = "tpch";
  sql.table = "lineitem";

  sql.table_filter = "";
  sql.table_batch_size = 2000;

  auto mysql_provider = std::make_shared<ral::io::mysql_data_provider>(sql);

  int rows = mysql_provider->get_num_handles();

  ral::io::mysql_parser parser;
  ral::io::Schema schema;
  auto handle = mysql_provider->get_next(false); // false so we make sure dont go to the db and get the schema info only
  parser.parse_schema(handle, schema);

  //std::vector<int> column_indices;
  //std::vector<int> column_indices = {0, 6};
  std::vector<int> column_indices = {0, 4};
  if (column_indices.empty()) {
    size_t num_cols = schema.get_num_columns();
    column_indices.resize(num_cols);
    std::iota(column_indices.begin(), column_indices.end(), 0);
  }
  mysql_provider->set_column_indices(column_indices);

  std::cout << "\trows: " << rows << "\n";
  handle = mysql_provider->get_next();
  auto res = handle.sql_handle.mysql_resultset;

  bool has_next = mysql_provider->has_next();
  std::cout << "\tNEXT?: " << (has_next?"TRUE":"FALSE") << "\n";


  std::cout << "\tTABLE\n";
  auto cols = schema.get_names();
  std::cout << "total cols: " << cols.size() << "\n";
  for (int i = 0; i < cols.size(); ++i) {
    std::cout << "\ncol: " << schema.get_name(i) << "\n";
    std::cout << "\ntyp: " << (int32_t)schema.get_dtype(i) << "\n";
  }

  std::cout << "\n\nCUDFFFFFFFFFFFFFFFFFFFFFF\n";

	std::vector<cudf::size_type> row_groups;

  std::unique_ptr<ral::frame::BlazingTable> bztbl = parser.parse_batch(handle, schema, column_indices, row_groups);
  ral::utilities::print_blazing_table_view(bztbl->toBlazingTableView(), "holis");
}


// TODO sqlite percy
