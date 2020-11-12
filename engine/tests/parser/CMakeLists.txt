set(parser_sources
    expression_tree_test.cpp
)

configure_test(parser_test "${parser_sources}")

set(parser_utils_sources
    expression_utils_test.cpp
)

configure_test(parser_utils_test "${parser_utils_sources}")

set(split_inequality_join_sources
    split_inequality_join_test.cpp
)
configure_test(split_inequality_join_test "${split_inequality_join_sources}")

set(parquet_metadata_sources
    parquet_metadata_test.cpp
)
configure_test(parquet_metadata_test "${parquet_metadata_sources}")