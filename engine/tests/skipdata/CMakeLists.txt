
set(PARQUET_FILE_PATH
    ${CMAKE_SOURCE_DIR}/tests/skipdata/simple.parquet)
 
set(skip_data_test_sources
    expression_tree_test.cpp
    minmax_parquet_test.cpp
)
configure_test(skip_data_test "${skip_data_test_sources}")
target_compile_definitions(skip_data_test
    PUBLIC -DPARQUET_FILE_PATH="${PARQUET_FILE_PATH}")