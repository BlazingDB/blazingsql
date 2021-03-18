#include "tests/utilities/BlazingUnitTest.h"
#include "io/data_provider/sql/MySQLDataProvider.h"
#include "io/data_parser/sql/MySQLParser.h"
#include <cudf_test/column_wrapper.hpp>
#include <cudf_test/column_utilities.hpp>
#include "utilities/DebuggingUtils.h"
#include <cudf_test/column_utilities.hpp> 

struct MySQLProviderTest : public BlazingUnitTest {};

TEST_F(MySQLProviderTest, select_all) {
  std::cout << "TEST\n";

	ral::io::sql_connection sql_conn = {
		.host = "localhost",
		.port = 33060,
		.user = "lucho",
		.password = "admin",
		.schema = "employees"};

  auto mysql_provider = std::make_shared<ral::io::mysql_data_provider>(sql_conn, "departments", 22);

  int rows = mysql_provider->get_num_handles();

  std::cout << "\trows: " << rows << "\n";
  auto handle = mysql_provider->get_next();
  auto res = handle.sql_handle.mysql_resultset;

  std::cout << "RESULTSET DATA -->>> TEST INICIOOOOOOOO -->> " << res->rowsCount() << "\n";  

  bool has_next = mysql_provider->has_next();
  std::cout << "\tNEXT?: " << (has_next?"TRUE":"FALSE") << "\n";
  
  
  std::cout << "\tTABLE\n";
//  while (res->next()) {
//    std::cout << "\t\t" << res->getString("dept_no") << "\n";
//  }

  std::cout << "PARSERRRRRRRRRRRRRRRRRRRRRRR\n";
  
  ral::io::mysql_parser parser;
  ral::io::Schema schema;

  std::cout << "RESULTSET DATA -->>> TEST antes de parse_schema -->> " << res->rowsCount() << "\n";  
  parser.parse_schema(handle, schema);
  
  auto cols = schema.get_names();
  std::cout << "total cols: " << cols.size() << "\n";
  for (int i = 0; i < cols.size(); ++i) {
    std::cout << "\ncol: " << schema.get_name(i) << "\n";
    std::cout << "\ntyp: " << (int32_t)schema.get_dtype(i) << "\n";
  }

  std::cout << "\n\nCUDFFFFFFFFFFFFFFFFFFFFFF\n";
  
  std::vector<int32_t> dat = {5, 4, 3, 5, 8, 5, 6, 5};
  std::vector<uint32_t> valy = {1, 1, 1, 1, 1, 1, 1, 1};
  cudf::test::fixed_width_column_wrapper<int32_t> vals(dat.begin(), dat.end(), valy.begin());
  std::unique_ptr<cudf::column> cudf_col = std::move(vals.release());

  auto col_string = cudf::test::to_string(cudf_col->view(), "|");
  
  std::cout << col_string << "\n";

  
  std::vector<int> column_indices(cols.size());
  for (int i = 0; i < column_indices.size(); ++i) {
    column_indices[i] = i;
  }
  
	std::vector<cudf::size_type> row_groups;

  std::cout << "RESULTSET DATA -->>> TEST antes de parse batch -->> " << res->rowsCount() << "\n";

  std::unique_ptr<ral::frame::BlazingTable> bztbl = parser.parse_batch(handle, schema, column_indices, row_groups);
  ral::utilities::print_blazing_table_view(bztbl->toBlazingTableView(), "holis");

//  if(mysql_provider->has_next()){
//      ral::io::data_handle new_handle;
//      try{
//          std::cout << "mysql_provider->get_next" << "\n";
//          new_handle = mysql_provider->get_next();
//          std::cout << "mysql_provider->get_next (DONE)" << "\n";
//      }
//      catch(...){
//          std::cout << "mysql_provider->get_next (FAIL)" << "\n";
//          FAIL();
//      }
//      try{
//          bool is_valid = new_handle.is_valid();
//          std::cout << "ral::io::data_handle.valid (DONE) " << is_valid << "\n";
          
//          bool empty_uri = new_handle.uri.isEmpty();
//          std::cout << "ral::io::data_handle.uri.isEmpty (DONE) " << empty_uri << "\n";
          
//          bool valid_uri = new_handle.uri.isValid();
//          std::cout << "ral::io::data_handle.uri.isValid (DONE) " << valid_uri << "\n";

//          bool null_filehandle = (new_handle.file_handle == nullptr);
//          std::cout << "ral::io::data_handle.uri.file_handle is null (DONE) " << null_filehandle << "\n";
          
//          bool empty_column_values = new_handle.column_values.empty();
//          std::cout << "ral::io::data_handle.column_values.empty (DONE) " << empty_column_values << "\n";
//      }
//      catch(...){
//          std::cout << "ral::io::data_handle ops (FAIL)" << "\n";
//          FAIL();
//      }
//  }
}
