set(parse_csv-test_SRCS
  parse_csv.cu
)

configure_test(parse_csv-test "${parse_csv-test_SRCS}")

find_library(CUDFTESTUTIL_LIBRARY "libcudftestutil.a"
             HINTS "$ENV{CUDF_ROOT}/lib")

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
)

configure_test(parse_orc-test "${parse_orc-test_SRCS}")

# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

set(parse_gdf-test_SRCS
    parse_gdf.cu
)

configure_test(parse_gdf-test "${parse_gdf-test_SRCS}")

