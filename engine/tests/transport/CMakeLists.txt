set(transport_files_SRC
    gpu-tcp-server-client-test.cc
)

configure_test(transport-test "${transport_files_SRC}")
