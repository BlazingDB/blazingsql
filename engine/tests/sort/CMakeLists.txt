set(source_files
    sort.cu
    ${CMAKE_SOURCE_DIR}/src/cudf/cpp/tests/utilities/table_utilities.cu    
)

configure_test(sort-test "${source_files}")
