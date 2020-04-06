#=============================================================================
# Copyright 2018 BlazingDB, Inc.
#     Copyright 2018 Percy Camilo Triveño Aucahuasi <percy@blazingdb.com>
#=============================================================================

#pass the dependency libraries as optional arguments using ${ARGN}
#NOTE the order of libraries matter, so try to link first with the most high level lib
function(configure_test TEST_NAME Tests_SRCS)
    include_directories(
        ${CMAKE_SOURCE_DIR}/src
        ${CMAKE_SOURCE_DIR}/tests/utils
        ${CMAKE_SOURCE_DIR}/thirdparty/jitify
        $ENV{PREFIX}/include
    )

    add_executable(${TEST_NAME}
                   ${Tests_SRCS}
                   ${CMAKE_SOURCE_DIR}/tests/cython_errors_dummy.cpp
                   ${CMAKE_SOURCE_DIR}/src/from_cudf/cpp_tests/utilities/column_utilities.cu)
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
        NVCategory
        NVStrings
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

        libboost_filesystem.a
        libboost_system.a
        libboost_regex.a

        protobuf

        # Library needed for blazing communication repository (debug)
        gcov
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

#add_subdirectory(logical-filter)
#add_subdirectory(calcite-interpreter)
#add_subdirectory(cudf/generator)
#add_subdirectory(nvcategory)
#add_subdirectory(integration)
#add_subdirectory(dtypes)
#add_subdirectory(pjoins)
#add_subdirectory(evaluate_query)
#add_subdirectory(order-by)
#add_subdirectory(gdf_column_cpp)
#add_subdirectory(where)
#add_subdirectory(joins)
#add_subdirectory(tpch)
#add_subdirectory(io-test)
#add_subdirectory(union)
#add_subdirectory(unary)
#add_subdirectory(groupbywoagg)

# # TODO Felipe JP
# #add_subdirectory(interpreter)

#add_subdirectory(utils)
#add_subdirectory(resultset-repository)
#add_subdirectory(parser)

add_subdirectory(logical-groupby)

add_subdirectory(transport)
add_subdirectory(skipdata)
add_subdirectory(new-tests)
# add_subdirectory(sort)
add_subdirectory(cache_machine)

message(STATUS "******** Tests are ready ********")
