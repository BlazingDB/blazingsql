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
    # TODO percy do not use libgdf.so here and use gdf (the static lib) instead
    target_link_libraries(${TEST_NAME}
        gmock_main
        gmock
        gtest

        blazingsql-engine
        ${PYTHON_LIBRARIES}

        blazingdb-io
        Threads::Threads
        blazingdb-transport

        cudf

        rmm
        zmq
        cudart
        cuda
        ${CUDA_CUDA_LIBRARY}
        ${CUDA_NVRTC_LIBRARY}
        ${CUDA_NVTX_LIBRARY}

        parquet
        arrow
        thrift
        snappy

        zstd
        lz4

        aws-cpp-sdk-s3
        aws-cpp-sdk-s3-encryption

        storage_client
        google_cloud_cpp_common
        crc32c

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


message(STATUS "******** Tests are ready ********")
