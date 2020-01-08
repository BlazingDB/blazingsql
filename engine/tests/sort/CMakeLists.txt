set(source_files
    sort.cu
    ${CMAKE_SOURCE_DIR}/src/cuDF/utilities/table_utilities.cu
)

configure_test(sort-test "${source_files}")
