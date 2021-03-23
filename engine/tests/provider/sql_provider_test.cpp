#include "tests/utilities/BlazingUnitTest.h"
#include "io/data_provider/sql/MySQLDataProvider.h"
#include "io/data_provider/sql/SQLiteDataProvider.h"
#include "io/data_parser/sql/MySQLParser.h"
#include "io/data_parser/sql/SQLiteParser.h"
#include <cudf_test/column_wrapper.hpp>
#include <cudf_test/column_utilities.hpp>
#include "utilities/DebuggingUtils.h"
#include <cudf_test/column_utilities.hpp> 

#include <sqlite3.h>

struct SQLProviderTest : public BlazingUnitTest {};

TEST_F(SQLProviderTest, mysql_select_all) {
	ral::io::sql_info sql;
  sql.host = "localhost";
  sql.port = 3306;
  sql.user = "blazing";
  sql.password = "admin";
  sql.schema = "bz3";
//  //sql.table = "departments";
  //sql.table = "DATABASECHANGELOG";
  sql.table = "new_table";
  //sql.table = "blazing_catalog_column_datatypes";
//  sql.table_filter = "";
//  sql.table_batch_size = 100;

//  sql.user = "lucho";
//  sql.password = "admin";
//  sql.schema = "employees";
//  //sql.table = "departments";
//  //sql.table = "employees";
//  sql.table = "dept_manager";
 

  sql.table_filter = "";
  sql.table_batch_size = 2000;

  auto mysql_provider = std::make_shared<ral::io::mysql_data_provider>(sql);

  int rows = mysql_provider->get_num_handles();

  ral::io::mysql_parser parser;
  ral::io::Schema schema;
  auto handle = mysql_provider->get_next(false); // false so we make sure dont go to the db and get the schema info only
  parser.parse_schema(handle, schema);

  //std::vector<int> column_indices = {2};
  std::vector<int> column_indices;
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
//TEST_F(SQLProviderTest, sqlite_select_all) {
//  std::cout << "TEST\n";

//	ral::io::sql_info sql;
//  sql.schema = "/home/percy/workspace/madona19/aucahuasi/bsql_Debug/feature_create-tables-from-rdbms/car_company_database/Car_Database.db";
//  sql.table = "Models";
//  sql.table_batch_size = 212;

//  auto mysql_provider = std::make_shared<ral::io::sqlite_data_provider>(sql);

//  int rows = mysql_provider->get_num_handles();

//  std::cout << "\trows: " << rows << "\n";
//  auto handle = mysql_provider->get_next();
//  auto res = handle.sql_handle.sqlite_statement;

//  bool has_next = mysql_provider->has_next();
//  std::cout << "\tNEXT?: " << (has_next?"TRUE":"FALSE") << "\n";
  
  
//  std::cout << "\tTABLE\n";
////  while (res->next()) {
////    std::cout << "\t\t" << res->getString("dept_no") << "\n";
////  }

//  std::cout << "PARSERRRRRRRRRRRRRRRRRRRRRRR\n";
  
//  ral::io::sqlite_parser parser;
//  ral::io::Schema schema;

//  parser.parse_schema(handle, schema);
  
//  auto cols = schema.get_names();
//  std::cout << "total cols: " << cols.size() << "\n";
//  for (int i = 0; i < cols.size(); ++i) {
//    std::cout << "\ncol: " << schema.get_name(i) << "\n";
//    std::cout << "\ntyp: " << (int32_t)schema.get_dtype(i) << "\n";
//  }

//  std::cout << "\n\nCUDFFFFFFFFFFFFFFFFFFFFFF\n";
  
////  std::vector<int32_t> dat = {5, 4, 3, 5, 8, 5, 6, 5};
////  std::vector<uint32_t> valy = {1, 1, 1, 1, 1, 1, 1, 1};
////  cudf::test::fixed_width_column_wrapper<int32_t> vals(dat.begin(), dat.end(), valy.begin());
////  std::unique_ptr<cudf::column> cudf_col = std::move(vals.release());

////  auto col_string = cudf::test::to_string(cudf_col->view(), "|");
  
////  std::cout << col_string << "\n";

  
//  std::vector<int> column_indices(cols.size());
//  for (int i = 0; i < column_indices.size(); ++i) {
//    column_indices[i] = i;
//  }

//	std::vector<cudf::size_type> row_groups;

//  std::unique_ptr<ral::frame::BlazingTable> bztbl = parser.parse_batch(handle, schema, column_indices, row_groups);
//  ral::utilities::print_blazing_table_view(bztbl->toBlazingTableView(), "holis");
  
  
  
  
  
  
  
  
////  auto mysql_provider = std::make_shared<ral::io::sqlite_data_provider>(sql_conn, "Customers", 212);

////  auto handle = mysql_provider->get_next();

////  std::cout << "COLUMNAS FOR SQLITE\n";
////  std::cout << "URI ----> : " << handle.uri.toString(false) << "\n";
////  for (int i = 0; i < handle.sql_handle.column_names.size(); ++i) {
////    auto col_name = handle.sql_handle.column_names[i];
////    auto col_type = handle.sql_handle.column_types[i];
////    std::cout << "\t" << col_name << " ( " << col_type << " )" << "\n";
////  }
  

////  auto stmt = handle.sql_handle.sqlite_statement.get();
////  int rc = 0;
////  while ((rc = sqlite3_step(stmt)) == SQLITE_ROW) {
////    const unsigned char *name = sqlite3_column_text(stmt, 1);
////    std::string col_name((char*)name);
////    std::cout << col_name << "  |  ";

////    auto type = sqlite3_column_int64(stmt, 6);
////    std::cout << type << "\n";
////  }
////  if (rc != SQLITE_DONE) {
////      printf("error: %s", "NOOOOO FALLO sqlite test");
////      // TODO percy error
////  }

////  std::cout << "OEFSQLITETEST\n";
//}
