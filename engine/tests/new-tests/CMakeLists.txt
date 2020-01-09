set(new_tests_SRCS
    apply_filter.cu
    ${CMAKE_SOURCE_DIR}/src/from_cudf/cpp_tests/utilities/table_utilities.cu
)

configure_test(apply_filter "${new_tests_SRCS}")


set(interops_test_SRCS
    interops_tests.cu
    ${CMAKE_SOURCE_DIR}/src/from_cudf/cpp_tests/utilities/table_utilities.cu
)

configure_test(interops_test "${interops_test_SRCS}")
