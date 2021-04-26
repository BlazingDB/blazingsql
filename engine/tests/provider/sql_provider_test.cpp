#include "io/data_parser/sql/MySQLParser.h"
#include "io/data_parser/sql/PostgreSQLParser.h"
#include "io/data_parser/sql/SQLiteParser.h"
#include "io/data_provider/sql/MySQLDataProvider.h"
#include "io/data_provider/sql/PostgreSQLDataProvider.h"
#include "io/data_provider/sql/SQLiteDataProvider.h"
#include "tests/utilities/BlazingUnitTest.h"
#include "utilities/DebuggingUtils.h"
#include <cudf_test/column_utilities.hpp>
#include <cudf_test/column_wrapper.hpp>

#include <sqlite3.h>

struct SQLProviderTest : public BlazingUnitTest {};

TEST_F(SQLProviderTest, DISABLED_postgresql_select_all) {
  ral::io::sql_info sql;
  sql.host = "localhost";
  sql.port = 5432;
  sql.user = "myadmin";
  sql.password = "";
  sql.schema = "pagila";
  sql.table = "prueba5";
  sql.table_filter = "";
  sql.table_batch_size = 2000;

  auto postgresql_provider =
      std::make_shared<ral::io::postgresql_data_provider>(sql, 1, 0);

  ral::io::postgresql_parser parser;
  ral::io::Schema schema;
  // false so we make sure dont go to the  db and get the schema info only
  auto handle = postgresql_provider->get_next(true);

  parser.parse_schema(handle, schema);

  std::unordered_map<cudf::type_id, const char *> dt2name{
      {cudf::type_id::INT8, "INT8"},
      {cudf::type_id::INT16, "INT16"},
      {cudf::type_id::INT32, "INT32"},
      {cudf::type_id::INT64, "INT64"},
      {cudf::type_id::UINT8, "UINT8"},
      {cudf::type_id::UINT16, "UINT16"},
      {cudf::type_id::UINT32, "UINT32"},
      {cudf::type_id::UINT64, "UINT64"},
      {cudf::type_id::FLOAT32, "FLOAT32"},
      {cudf::type_id::FLOAT64, "FLOAT64"},
      {cudf::type_id::DECIMAL64, "DECIMAL64"},
      {cudf::type_id::BOOL8, "BOOL8"},
      {cudf::type_id::STRING, "STRING"},
  };

  std::cout << "SCHEMA" << std::endl
            << "  length = " << schema.get_num_columns() << std::endl
            << "  columns" << std::endl;
  for(std::size_t i = 0; i < schema.get_num_columns(); i++) {
    const std::string & name = schema.get_name(i);
    std::cout << "    " << name << ": ";
    try {
      const std::string dtypename = dt2name[schema.get_dtype(i)];
      std::cout << dtypename << std::endl;
    } catch(std::exception &) {
      std::cout << static_cast<int>(schema.get_dtype(i)) << std::endl;
    }
  }

  auto num_cols = schema.get_num_columns();

  std::vector<int> column_indices(num_cols);
    std::iota(column_indices.begin(), column_indices.end(), 0);

  std::vector<cudf::size_type> row_groups;
  auto table = parser.parse_batch(handle, schema, column_indices, row_groups);

  std::cout << "TABLE" << std::endl
            << " ncolumns =  " << table->num_columns() << std::endl
            << " nrows =  " << table->num_rows() << std::endl;
}


void print_batch(const ral::io::data_handle & handle,
    const ral::io::Schema & schema,
    ral::io::mysql_parser & parser,
    const std::vector<int> & column_indices) {
  std::vector<cudf::size_type> row_groups;
  std::unique_ptr<ral::frame::BlazingTable> bztbl =
      parser.parse_batch(handle, schema, column_indices, row_groups);
  static int i = 0;
  ral::utilities::print_blazing_table_view(
      bztbl->toBlazingTableView(), "holis" + std::to_string(++i));
  std::cout << "TREMINO DE IMPRIMER CUDF TABLE!!! \n";
}

TEST_F(SQLProviderTest, DISABLED_mysql_select_all) {
  ral::io::sql_info sql;
  sql.host = "localhost";
  //sql.port = 5432; // pg
  sql.port = 3306;
  //  sql.user = "blazing";
  //  sql.password = "admin";
  //  sql.schema = "bz3";
  //  //sql.table = "departments";
  // sql.table = "DATABASECHANGELOG";
  // sql.table = "new_table";
  // sql.table = "blazing_catalog_column_datatypes";
  //  sql.table_filter = "";
  //  sql.table_batch_size = 100;

  sql.user = "lucho";
  sql.password = "admin";
  sql.schema = "employees";
  // sql.table = "departments";
  sql.table = "employees";
  // sql.table = "dept_manager";


  sql.schema = "tpch";
  sql.table = "lineitem";
  //sql.table = "nation";
  //sql.table = "orders";

  sql.table_filter = "";
  sql.table_batch_size = 200000;
  sql.table_batch_size = 2;

  auto mysql_provider =
      std::make_shared<ral::io::mysql_data_provider>(sql, 1, 0);

  int rows = mysql_provider->get_num_handles();

  ral::io::mysql_parser parser;
  ral::io::Schema schema;
  auto handle =
      mysql_provider->get_next(false);  // false so we make sure dont go to
                                        // the db and get the schema info only
  parser.parse_schema(handle, schema);

  std::vector<int> column_indices;
  // std::vector<int> column_indices = {0, 6};
  // std::vector<int> column_indices = {0, 4}; // line item id fgloat
  // std::vector<int> column_indices = {4}; // line item fgloat
  // std::vector<int> column_indices = {8};  // line item ret_flag
  // std::vector<int> column_indices = {1}; // nation 1 name
  if(column_indices.empty()) {
    size_t num_cols = schema.get_num_columns();
    column_indices.resize(num_cols);
    std::iota(column_indices.begin(), column_indices.end(), 0);
  }
  mysql_provider->set_column_indices(column_indices);

  //std::string exp = "BindableTableScan(table=[[main, lineitem]], filters=[[OR(AND(>($0, 599990), <=($3, 1998-09-02)), AND(<>(-($0, 1), +(65, /(*(*(98, $0), 2), 3))), IS NOT NULL($1)))]], projects=[[0, 1, 9, 10]], aliases=[[l_orderkey, l_partkey, l_linestatus, l_shipdate]])";
  //std::string exp = "BindableTableScan(table=[[main, orders]], filters=[[NOT(LIKE($2, '%special%requests%'))]], projects=[[0, 1, 8]], aliases=[[o_orderkey, o_custkey, o_comment]])";
  //std::string exp = "BindableTableScan(table=[[main, lineitem]], filters=[[AND(OR(=($4, 'MAIL'), =($4, 'SHIP')), <($2, $3), <($1, $2), >=($3, 1994-01-01), <($3, 1995-01-01))]], projects=[[0, 10, 11, 12, 14]], aliases=[[l_orderkey, l_shipdate, l_commitdate, l_receiptdate, l_shipmode]])";
  std::string exp = "BindableTableScan(table=[[main, lineitem]], filters=[[AND(>=($3, 1995-09-01), <($3, 1995-10-01))]], projects=[[1, 5, 6, 10]], aliases=[[l_partkey, l_extendedprice, l_discount, l_shipdate]])";


  mysql_provider->set_predicate_pushdown(exp);

  std::cout << "\tTABLE\n";
  auto cols = schema.get_names();
  std::cout << "total cols: " << cols.size() << "\n";
  for(int i = 0; i < cols.size(); ++i) {
    std::cout << "\ncol: " << schema.get_name(i) << "\n";
    std::cout << "\ntyp: " << (int32_t) schema.get_dtype(i) << "\n";
  }

  std::cout << "\n\nCUDFFFFFFFFFFFFFFFFFFFFFF\n";

  bool only_once = false;
  if(only_once) {
    std::cout << "\trows: " << rows << "\n";
    handle = mysql_provider->get_next();
    auto res = handle.sql_handle.mysql_resultset;

    bool has_next = mysql_provider->has_next();
    std::cout << "\tNEXT?: " << (has_next ? "TRUE" : "FALSE") << "\n";
    print_batch(handle, schema, parser, column_indices);
  } else {
    mysql_provider->reset();
    while(mysql_provider->has_next()) {
      handle = mysql_provider->get_next();
      print_batch(handle, schema, parser, column_indices);
    }
  }
}

TEST_F(SQLProviderTest, DISABLED_sqlite_select_all) {
  ral::io::sql_info sql;
  sql.schema = "/blazingsql/db.sqlite3";
  sql.table = "prueba2";
  sql.table_filter = "";
  sql.table_batch_size = 2000;

  auto sqlite_provider =
      std::make_shared<ral::io::sqlite_data_provider>(sql, 1, 0);

  ral::io::sqlite_parser parser;
  ral::io::Schema schema;
  auto handle = sqlite_provider->get_next(true);

  parser.parse_schema(handle, schema);

  std::unordered_map<cudf::type_id, const char *> dt2name{
      {cudf::type_id::INT8, "INT8"},
      {cudf::type_id::INT16, "INT16"},
      {cudf::type_id::INT32, "INT32"},
      {cudf::type_id::INT64, "INT64"},
      {cudf::type_id::UINT8, "UINT8"},
      {cudf::type_id::UINT16, "UINT16"},
      {cudf::type_id::UINT32, "UINT32"},
      {cudf::type_id::UINT64, "UINT64"},
      {cudf::type_id::FLOAT32, "FLOAT32"},
      {cudf::type_id::FLOAT64, "FLOAT64"},
      {cudf::type_id::DECIMAL64, "DECIMAL64"},
      {cudf::type_id::BOOL8, "BOOL8"},
      {cudf::type_id::STRING, "STRING"},
  };

  std::cout << "SCHEMA" << std::endl
            << "  length = " << schema.get_num_columns() << std::endl
            << "  columns" << std::endl;
  for(std::size_t i = 0; i < schema.get_num_columns(); i++) {
    const std::string & name = schema.get_name(i);
    std::cout << "    " << name << ": ";
    try {
      const std::string dtypename = dt2name[schema.get_dtype(i)];
      std::cout << dtypename << std::endl;
    } catch(std::exception &) {
      std::cout << static_cast<int>(schema.get_dtype(i)) << std::endl;
    }
  }

  auto num_cols = schema.get_num_columns();

  std::vector<int> column_indices(num_cols);
  std::iota(column_indices.begin(), column_indices.end(), 0);

  std::vector<cudf::size_type> row_groups;
  auto table = parser.parse_batch(handle, schema, column_indices, row_groups);

  std::cout << "TABLE" << std::endl
            << " ncolumns =  " << table->num_columns() << std::endl
            << " nrows =  " << table->num_rows() << std::endl;

  auto tv = table->toBlazingTableView();

  for(cudf::size_type i = 0; i < static_cast<cudf::size_type>(num_cols); i++) {
    cudf::test::print(tv.column(i));
  }
}
