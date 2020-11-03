#=============================================================================
# Copyright 2018 BlazingDB, Inc.
#     Copyright 2018 Percy Camilo Triveño Aucahuasi <percy@blazingdb.com>
#=============================================================================

#pass the dependency libraries as optional arguments using ${ARGN}
#NOTE the order of libraries matter, so try to link first with the most high level lib
function(configure_test TEST_NAME Tests_SRCS)
    include_directories(
        ${CMAKE_SOURCE_DIR}/src
        ${CMAKE_SOURCE_DIR}/tests/utilities
        ${CMAKE_SOURCE_DIR}/thirdparty/jitify
        $ENV{PREFIX}/include
    )

    add_executable(${TEST_NAME}
                   ${Tests_SRCS}
                   ${CMAKE_SOURCE_DIR}/tests/cython_errors_dummy.cpp
                   ${CMAKE_SOURCE_DIR}/tests/utilities/BlazingUnitTest.cpp)
    link_directories($ENV{PREFIX}/lib)
    
    target_link_libraries(${TEST_NAME}
        gmock_main
        gmock
        gtest

        blazingsql-engine
        ${PYTHON_LIBRARIES}

        blazingdb-io
        Threads::Threads
        blazingdb-transport

        # TODO: Make this cleaner. Here we just used to link cudf, but cudf got split into a whole bunch of libraries in this PR https://github.com/rapidsai/cudf/pull/6134
        # cudf
        libcudf_base.so
        libcudf_rolling.so
        libcudf_replace.so
        libcudf_transpose.so
        libcudf_reductions.so
        libcudf_ast.so
        libcudf_join.so
        libcudf_reshape.so
        libcudf_merge.so
        libcudf_comms.so
        libcudf_hash.so
        libcudf_io.so
        libcudf_interop.so
        libcudf_partitioning.so


        zmq
        cudart

        parquet
        arrow
        snappy

        zstd
        lz4

        ${S3_LIBRARY}

        ${GCS_LIBRARY}

        libboost_filesystem.so
        libboost_system.so
        libboost_regex.so

        protobuf

        # Library needed for blazing communication repository (debug)
        gcov

        # TODO jp c.gonzales make use of standard cmake find_package here (see above)
        #spdlog::spdlog
        libspdlog.a

        cudftestutil
    )

    set_target_properties(${TEST_NAME} PROPERTIES
        RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/gtests/")

    #register the target as CMake test so we can use ctest for this executable
    add_test(NAME ${TEST_NAME} COMMAND ${TEST_NAME})
endfunction()

## Main ##

enable_testing()

# Configure unit tests with Google Test
# -------------------
message(STATUS "******** Configuring tests ********")

add_subdirectory(cache_machine)
add_subdirectory(logical-groupby)
add_subdirectory(new-tests)
add_subdirectory(parser)
add_subdirectory(skipdata)
add_subdirectory(sort)
add_subdirectory(waiting_queue)
add_subdirectory(kernel_tests)
add_subdirectory(provider)
add_subdirectory(logic_controllers)

message(STATUS "******** Tests are ready ********")
