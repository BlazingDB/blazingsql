set(new_tests_SRCS
    apply_filter.cpp
)

configure_test(apply_filter "${new_tests_SRCS}")

# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

set(interops_test_SRCS
    interops_tests.cpp
)

configure_test(interops_test "${interops_test_SRCS}")

# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

set(project_test_SRCS
    process_project.cpp
    ${CMAKE_SOURCE_DIR}/src/execution_graph/logic_controllers/LogicalProject.cpp
)

configure_test(project_test "${project_test_SRCS}")

# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

set(filter_test_SRCS
    filter_tests.cpp
)

configure_test(filter_test "${filter_test_SRCS}")

# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

set(sample-generator_SRCS sample-generator.cpp)

configure_test(sample-generator "${sample-generator_SRCS}")
