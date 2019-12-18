set(interpreter_tests_src
    interpreter_tests.cu
)

configure_test(interpreter_tests "${interpreter_tests_src}")

set(project_tests_src
    process_project_tests.cu
)

configure_test(project_tests "${project_tests_src}")

set(project_coalesce_tests_src
    interops_coalesce_test.cu
)

configure_test(project_coalesce_tests "${project_coalesce_tests_src}")
