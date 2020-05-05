set(transport_files_SRC
    utils/column_factory.cu
    gpu-batch-tcp-server-client-test.cc
)

configure_test(transport-test "${transport_files_SRC}")
