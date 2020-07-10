set(cache_test_sources
        generators/file_generator.cpp
        #../utilities/MemoryConsumer.cu
        cache_test.cpp
        batching.cpp
        #memory_consumer_test.cpp
)
# TODO jp rommel c.cordova fix this tests
configure_test(cache_test "${cache_test_sources}")
