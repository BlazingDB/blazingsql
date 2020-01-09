set(parse_csv-test_SRCS
  parse_csv.cu
)
  
set(parse_parquet-test_SRCS
    parse_parquet.cu
)
 
configure_test(parse_csv-test "${parse_csv-test_SRCS}")


set(PARQUET_FILE_PATH
    ${CMAKE_SOURCE_DIR}/tests/io-test/simple.parquet)

configure_test(parse_parquet-test "${parse_parquet-test_SRCS}")

target_compile_definitions(parse_parquet-test
    PUBLIC -DPARQUET_FILE_PATH="${PARQUET_FILE_PATH}")
