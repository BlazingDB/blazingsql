set(aggregation-test_SRCS
    aggregation-test.cu
    ${CMAKE_SOURCE_DIR}/src/from_cudf/cpp_tests/utilities/table_utilities.cu
)

#configure_test(aggregation-test "${aggregation-test_SRCS}")
