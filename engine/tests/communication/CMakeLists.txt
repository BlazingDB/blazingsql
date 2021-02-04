
set(send_and_receive_test_SRCS
send_and_receive_test.cpp
)

configure_test(send_and_receive_test "${send_and_receive_test_SRCS}")

set(send_and_receive_test_ucx_SRCS
send_and_receive_test_ucx.cpp
)

configure_test(send_and_receive_test_ucx "${send_and_receive_test_ucx_SRCS}")
