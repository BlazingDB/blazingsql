set(new_tests_SRCS
    apply_filter.cu
    ${CMAKE_SOURCE_DIR}/src/from_cudf/cpp_tests/utilities/table_utilities.cu    
)

configure_test(apply_filter "${new_tests_SRCS}")
