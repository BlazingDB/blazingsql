set(message_receiver_tests_SRCS
    message_receiver_tests.cpp
)

configure_test(message_receiver_test "${message_receiver_tests_SRCS}")

set(message_sender_tests_SRCS
    message_sender_tests.cpp
)

configure_test(message_sender_test "${message_sender_tests_SRCS}")



# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
