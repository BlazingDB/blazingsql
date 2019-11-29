#include <type_traits>

#include <gtest/gtest.h>

#include <ResultSetRepository.h>
#include <GDFColumn.cuh>

#include "../utils/gdf/library/table_group.h"
#include <DataFrame.h>
#include <vector>


class ResultSetRepositoryTest : public ::testing::Test {
  virtual void SetUp() {
    rmmInitialize(nullptr);
    connection = result_set_repository::get_instance().init_session();
    token = result_set_repository::get_instance().register_query(connection, 0);

  }

  virtual void TearDown(){
    result_set_repository::get_instance().remove_all_connection_tokens(connection);
  }
protected:
  connection_id_t connection;
  query_token_t token;
};

TEST_F(ResultSetRepositoryTest, basic_resulset_test) {

  {

    gdf_column_cpp column;
  	gdf_dtype_extra_info extra_info{TIME_UNIT_NONE};
    column.create_gdf_column(GDF_INT32,extra_info,100,nullptr,4);
    blazing_frame frame;
    std::vector<gdf_column_cpp> columns;
    columns.push_back(column.clone());
    frame.add_table(columns);
    result_set_repository::get_instance().update_token(token, frame , .01);

    result_set_t result = result_set_repository::get_instance().get_result(connection,token);
    EXPECT_TRUE(result.is_ready);
    result_set_repository::get_instance().free_result(connection,token);

    try {
      result_set_repository::get_instance().get_result(connection,token);
        EXPECT_TRUE(false);
       }
       catch(std::runtime_error const & err) {
           EXPECT_EQ(err.what(),std::string("Result set does not exist"));
       }

  }
}


TEST_F(ResultSetRepositoryTest, string_resulset_test) {

  {
    int num_strings = 100;
    const char ** char_array = new const char *[num_strings];
    for(int i = 0; i < num_strings; i++){
      char_array[i] = "test!";
    }

    char* name = (char*)"some_name";
    NVStrings * string = NVStrings::create_from_array(char_array,num_strings);
    gdf_column * col_struct = new gdf_column;
    col_struct->dtype = GDF_STRING;
    col_struct->size = string->size();
    col_struct->valid = nullptr;
    col_struct->data = (void *) string;
    col_struct->col_name = name;
    gdf_column_cpp column;
    column.create_gdf_column(col_struct);
    blazing_frame frame;
    std::vector<gdf_column_cpp> columns;
    columns.push_back(column);
    frame.add_table(columns);

    result_set_repository::get_instance().update_token(token, frame , .01);

    result_set_t result = result_set_repository::get_instance().get_result(connection,token);
    EXPECT_TRUE(result.is_ready);
    result_set_repository::get_instance().free_result(connection,token);

    try {
        result = result_set_repository::get_instance().get_result(connection,token);
        EXPECT_TRUE(false);
       }
       catch(std::runtime_error const & err) {
           EXPECT_EQ(err.what(),std::string("Result set does not exist"));
       }

       for(int i = 0; i < 100; i++){
       //   delete[] char_array[i];
       }
       delete[] char_array;
  }
}
