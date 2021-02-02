# set(message_receiver_tests_SRCS
#     message_receiver_tests.cpp
# )

# configure_test(message_receiver_test "${message_receiver_tests_SRCS}")

# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

set(message_sender_tests_SRCS
    message_sender_tests.cpp
)

configure_test(message_sender_test "${message_sender_tests_SRCS}")

# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

# set(message_send_receive_tests_SRCS
#     message_send_receive.cpp
# )

# configure_test(message_send_receive_test "${message_send_receive_tests_SRCS}")

# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

set(send_and_receive_test_SRCS
send_and_receive_test.cpp
)

configure_test(send_and_receive_test "${send_and_receive_test_SRCS}")
