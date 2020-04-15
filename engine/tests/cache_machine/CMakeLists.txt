set(cache_test_sources
        generators/file_generator.cu
        ../utilities/MemoryConsumer.cu

        cache_test.cu
        graph_processor_test.cpp
        # expr_to_graph_processor.cpp
        # batching.cpp
        # memory_consumer_test.cpp
)
configure_test(cache_test "${cache_test_sources}")
