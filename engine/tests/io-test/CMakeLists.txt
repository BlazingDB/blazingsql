set(parse_csv-test_SRCS
  parse_csv.cu
)

configure_test(parse_csv-test "${parse_csv-test_SRCS}")

# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
  
set(parse_parquet-test_SRCS
    parse_parquet.cu
)

set(PARQUET_FILE_PATH
    ${CMAKE_SOURCE_DIR}/tests/io-test/simple.parquet)

configure_test(parse_parquet-test "${parse_parquet-test_SRCS}")

target_compile_definitions(parse_parquet-test
    PUBLIC -DPARQUET_FILE_PATH="${PARQUET_FILE_PATH}")

# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

set(parse_orc-test_SRCS
    parse_orc.cu
    ${CMAKE_SOURCE_DIR}/src/from_cudf/cpp_tests/utilities/table_utilities.cu
)

configure_test(parse_orc-test "${parse_orc-test_SRCS}")

# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

set(parse_gdf-test_SRCS
    parse_gdf.cu
    ${CMAKE_SOURCE_DIR}/src/from_cudf/cpp_tests/utilities/table_utilities.cu
)

configure_test(parse_gdf-test "${parse_gdf-test_SRCS}")

