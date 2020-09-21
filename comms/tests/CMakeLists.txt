#=============================================================================
# Copyright 2018 BlazingDB, Inc.
#     Copyright 2018 Percy Camilo Triveño Aucahuasi <percy@blazingdb.com>
#=============================================================================

#pass the dependency libraries as optional arguments using ${ARGN}
#NOTE the order of libraries matter, so try to link first with the most high level lib
function(configure_test TEST_NAME Tests_SRCS)
    ###################################################################################################
    # - common test utils -----------------------------------------------------------------------------

    set(CUDF_TEST_INCLUDE "$ENV{CUDF_HOME}/cpp")

    find_library(CUDFTESTUTIL_LIBRARY "libcudftestutil.a"
                HINTS "$ENV{CUDF_ROOT}/lib")

    message(STATUS "CUDFTESTUTIL: CUDFTESTUTIL_LIBRARY set to ${CUDFTESTUTIL_LIBRARY}")
    message(STATUS "CUDFTESTUTIL: CUDF_TEST_INCLUDE set to ${CUDF_TEST_INCLUDE}")

    add_library(cudftestutil STATIC IMPORTED ${CUDFTESTUTIL_LIBRARY})

    if (CUDF_TEST_INCLUDE AND CUDFTESTUTIL_LIBRARY)
        set_target_properties(cudftestutil PROPERTIES IMPORTED_LOCATION ${CUDFTESTUTIL_LIBRARY})
    endif (CUDF_TEST_INCLUDE AND CUDFTESTUTIL_LIBRARY)

    ###################################################################################################
    
    include_directories(
        ${CMAKE_SOURCE_DIR}/src
        ${CMAKE_SOURCE_DIR}/tests/utils
        ${CMAKE_SOURCE_DIR}/thirdparty/jitify
        $ENV{PREFIX}/include
        ${CUDF_TEST_INCLUDE}
    )

    add_executable(${TEST_NAME}
                   ${Tests_SRCS})
    link_directories($ENV{PREFIX}/lib)
    # TODO percy do not use libgdf.so here and use gdf (the static lib) instead
    target_link_libraries(${TEST_NAME}
        gmock_main
        gmock
        gtest

        blazingdb-transport

        Threads::Threads
        cudart
        cuda
        zmq
        ${CUDA_CUDA_LIBRARY}
        ${CUDA_NVRTC_LIBRARY}
        ${CUDA_NVTX_LIBRARY}

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

set(logical_filter_test_SRCS
    #utils/Traits/RuntimeTraits.cpp
    integration-server-client-test.cc
    node-test.cc
)

configure_test(blazingdb-transport-test "${logical_filter_test_SRCS}")

message(STATUS "******** Tests are ready ********")

add_custom_target(coverage
        COMMAND lcov -c -d ${CMAKE_BINARY_DIR}/src -o coverage.info
        COMMAND lcov -r coverage.info '/usr*' '*boost*' '*build*' -o coverage.info
        COMMAND genhtml coverage.info -o ${CMAKE_BINARY_DIR}/coverage-html)
