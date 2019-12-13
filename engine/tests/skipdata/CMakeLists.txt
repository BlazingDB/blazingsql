set(skip_data_test_sources
    expression_tree_test.cpp
    minmax_parquet_test.cpp
#    host_column_test.cpp
)
configure_test(skip_data_test "${skip_data_test_sources}")
