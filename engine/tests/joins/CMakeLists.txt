set(full-test_SRCS
  fullouter_join.cu
)

configure_test(fullouter_join-test "${full-test_SRCS}")

set(left-test_SRCS
  leftouter_join.cu
)

configure_test(leftouter_join-test "${left-test_SRCS}")
