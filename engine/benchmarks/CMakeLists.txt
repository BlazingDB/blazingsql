#=============================================================================
# Copyright 2018 BlazingDB, Inc.
#     Copyright 2018 Percy Camilo Triveño Aucahuasi <percy@blazingdb.com>
#=============================================================================

#pass the dependency libraries as optional arguments using ${ARGN}
#NOTE the order of libraries matter, so try to link first with the most high level lib
function(configure_benchmark BENCHMARK_NAME Bench_SRCS)
    include_directories(${CMAKE_SOURCE_DIR}/src ${CMAKE_SOURCE_DIR}/tests/utils ${GBENCHMARK_INCLUDE_DIR})
    link_directories(${GBENCHMARK_LIBRARY_DIR})

    add_executable(${BENCHMARK_NAME} ${Bench_SRCS})

    # TODO percy do not use libgdf.so here and use gdf (the static lib) instead
    target_link_libraries(${BENCHMARK_NAME}
        benchmark
        benchmark_main

        blazingsql-engine
        blazingdb-io
        blazingdb-transport
    )

    set_target_properties(${BENCHMARK_NAME} PROPERTIES
        RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/gbenchmarks/")

endfunction()

## Main ##

# Configure benchmarks with Google Benchmark
# -------------------
message(STATUS "******** Configuring Benchmarks ********")


add_subdirectory(jit)
add_subdirectory(interops)


message(STATUS "******** Benchmarks are ready ********")
