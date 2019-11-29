set(gdf_column_cpp_test_SRCS
    gdf_column_cpp_test.cu
    ${CMAKE_SOURCE_DIR}/tests/utilities/gdf_column_cpp_utilities.cu
)

configure_test(gdf_column_cpp_test "${gdf_column_cpp_test_SRCS}")
