set(calcite_interpreter_test_SRCS
    calcite_interpreter-test.cu
)

set(calcite_interpreter_where_test_SRCS
    calcite_interpreter-where-test.cu
)

configure_test(calcite-interpreter-test "${calcite_interpreter_test_SRCS}")
configure_test(calcite-interpreter-where-test "${calcite_interpreter_where_test_SRCS}")
