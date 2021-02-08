set(cache_test_sources
        cache_test.cpp
)
configure_test(cache_test "${cache_test_sources}")

set(exception_handling_test_sources
        ../utilities/MemoryConsumer.cu
        exception_handling_test.cpp
)
configure_test(exception_handling_test "${exception_handling_test_sources}")
