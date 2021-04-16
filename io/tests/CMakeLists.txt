#=============================================================================
# Copyright 2018 BlazingDB, Inc.
#     Copyright 2018 Percy Camilo Triveño Aucahuasi <percy@blazingdb.com>
#=============================================================================

#pass the dependency libraries as optional arguments using ${ARGN}
#NOTE the order of libraries matter, so try to link first with the most high level lib
function(configure_test TEST_NAME Tests_SRCS)
    add_executable(${TEST_NAME} ${Tests_SRCS})

    link_directories($ENV{CONDA_PREFIX}/lib)
    include_directories($ENV{CONDA_PREFIX}/include)
    target_link_libraries(${TEST_NAME}
        gmock
        gmock_main
        gtest
        blazingdb-io
        ${S3_LIBRARY}
        ${GCS_LIBRARY}
        arrow

        libabsl_time.so
        libabsl_str_format_internal.so
        libabsl_strings.so
        libabsl_strings_internal.so
        libabsl_time_zone.so
        libabsl_base.so
        libabsl_int128.so
        libabsl_raw_logging_internal.so
        libabsl_spinlock_wait.so
        libabsl_bad_variant_access.so
        libabsl_civil_time.so
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

# TODO percy fix tests
add_subdirectory(ExceptionHandling)
add_subdirectory(FileSystem)
#add_subdirectory(Library)
add_subdirectory(Util)

message(STATUS "******** Tests are ready ********")
