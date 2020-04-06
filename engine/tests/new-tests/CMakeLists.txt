set(new_tests_SRCS
    apply_filter.cu
    ${CMAKE_SOURCE_DIR}/src/from_cudf/cpp_tests/utilities/table_utilities.cu
)

configure_test(apply_filter "${new_tests_SRCS}")

# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

set(interops_test_SRCS
    interops_tests.cu
    ${CMAKE_SOURCE_DIR}/src/from_cudf/cpp_tests/utilities/table_utilities.cu
)

configure_test(interops_test "${interops_test_SRCS}")

# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

set(project_test_SRCS
    process_project.cu
    ${CMAKE_SOURCE_DIR}/src/from_cudf/cpp_tests/utilities/table_utilities.cu
    ${CMAKE_SOURCE_DIR}/src/execution_graph/logic_controllers/LogicalProject.cpp
)

#configure_test(project_test "${project_test_SRCS}")

# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

set(filter_test_SRCS
    filter_tests.cu
    ${CMAKE_SOURCE_DIR}/src/from_cudf/cpp_tests/utilities/table_utilities.cu
)

configure_test(filter_test "${filter_test_SRCS}")

# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

set(sample-generator_SRCS sample-generator.cu)

configure_test(sample-generator "${sample-generator_SRCS}")
