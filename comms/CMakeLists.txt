#=============================================================================
# Copyright 2019 BlazingDB, Inc.
#     Copyright 2019 Percy Camilo Triveño Aucahuasi <percy@blazingdb.com>
#=============================================================================

cmake_minimum_required(VERSION 3.12 FATAL_ERROR)

project(blazingdb-communication C CXX CUDA)

set(CMAKE_MODULE_PATH "${CMAKE_SOURCE_DIR}/cmake/" ${CMAKE_MODULE_PATH})
message(STATUS "CMAKE_MODULE_PATH: ${CMAKE_MODULE_PATH}")
set(CMAKE_POSITION_INDEPENDENT_CODE ON)

###################################################################################################
# - conda environment -----------------------------------------------------------------------------
set(BSQL_BLD_PREFIX "")
if ("$ENV{CONDA_BUILD}" STREQUAL "1")
    set(CMAKE_SYSTEM_PREFIX_PATH "$ENV{BUILD_PREFIX};$ENV{PREFIX};${CMAKE_SYSTEM_PREFIX_PATH}")
    message(STATUS "Conda build detected, CMAKE_SYSTEM_PREFIX_PATH set to: ${CMAKE_SYSTEM_PREFIX_PATH}")
    set(BSQL_BLD_PREFIX "$ENV{BUILD_PREFIX}")
elseif (DEFINED ENV{CONDA_PREFIX})
    set(CMAKE_SYSTEM_PREFIX_PATH "$ENV{CONDA_PREFIX};${CMAKE_SYSTEM_PREFIX_PATH}")
    message(STATUS "Conda environment detected, CMAKE_SYSTEM_PREFIX_PATH set to: ${CMAKE_SYSTEM_PREFIX_PATH}")
    set(BSQL_BLD_PREFIX "$ENV{CONDA_PREFIX}")
endif ()
message(WARNING "BSQL_BLD_PREFIX set to: ${BSQL_BLD_PREFIX}")

#########################################################################3
# - compiler stuff ------------------
set(CMAKE_CXX_STANDARD 14)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

set(CMAKE_CUDA_STANDARD 14)
set(CMAKE_CUDA_STANDARD_REQURIED ON)

message(STATUS "Using C++ standard: c++${CMAKE_CXX_STANDARD}")

if (CMAKE_COMPILER_IS_GNUCXX)
    option(CXX_OLD_ABI "Disable the GLIBCXX11 ABI" OFF)
    if (CXX_OLD_ABI)
        message(STATUS "Disabling the GLIBCXX11 ABI")
        set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -D_GLIBCXX_USE_CXX11_ABI=0")
        set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -D_GLIBCXX_USE_CXX11_ABI=0")
        set(CMAKE_CUDA_FLAGS "${CMAKE_CUDA_FLAGS} -Xcompiler -D_GLIBCXX_USE_CXX11_ABI=0")
    else ()
        message(STATUS "Enabling the GLIBCXX11 ABI")
    endif (CXX_OLD_ABI)
endif (CMAKE_COMPILER_IS_GNUCXX)

include(DefineVersions)

# Include CMake modules
include(FeatureSummary)
include(CheckIncludeFiles)
include(CheckLibraryExists)
find_package(Threads)
find_package(GTest REQUIRED)
include(CTest)

# Configure CUDA
include(ConfigureCUDA)

# TODO percy manage cuda definitions
add_definitions(${CXX_DEFINES})

include_directories(
        ${CMAKE_SOURCE_DIR} ${CMAKE_BINARY_DIR}
        ${CMAKE_CUDA_TOOLKIT_INCLUDE_DIRECTORIES}
        ${RAPIDJSON_INCLUDE_DIRS}
        ${CMAKE_SOURCE_DIR}/src
        ${CMAKE_SOURCE_DIR}/include
        ${BSQL_BLD_PREFIX}/include)

link_directories(${BSQL_BLD_PREFIX}/lib)

function(blazingdb_artifact)
    set(OPTIONS OPTIONAL VIRTUAL)
    set(ONE_VALUE_ARGS PREFIX TARGET)
    set(MULTI_VALUE_ARGS SOURCES INCLUDES LIBRARIES PUBLIC_HEADERS TESTS)
    cmake_parse_arguments(ARG "${OPTIONS}" "${ONE_VALUE_ARGS}" "${MULTI_VALUE_ARGS}" ${ARGN})
    if (ARG_UNPARSED_ARGUMENTS)
        message(SEND_ERROR
                "Error: unrecognized arguments: ${ARG_UNPARSED_ARGUMENTS}")
    endif ()

    if (${ARG_VIRTUAL})
        set(TYPE OBJECT)
    endif ()


    if (ARG_PUBLIC_HEADERS)
        message(STATUS "Log: public headers ${ARG_PUBLIC_HEADERS}/blazingdb")
        install(DIRECTORY ${ARG_PUBLIC_HEADERS}/blazingdb DESTINATION include)
    endif ()

    add_library(${ARG_TARGET} SHARED ${ARG_SOURCES})

    if (ARG_LIBRARIES)
        target_link_libraries(${ARG_TARGET} ${ARG_LIBRARIES})
    endif ()

    if (ARG_INCLUDES)
        target_include_directories(${ARG_TARGET} ${ARG_INCLUDES})
    endif ()

    if (NOT ARG_PREFIX)
        set(ARG_PREFIX "")
    endif ()

    install(TARGETS ${ARG_TARGET}
            LIBRARY DESTINATION lib
            ARCHIVE DESTINATION lib)

    message(STATUS "Log: BUILD_TESTS ${BUILD_TESTS}")

    if (BUILD_TESTS AND GTEST_FOUND AND ARG_TESTS)
        message(STATUS "Log: unit test ${ARG_TARGET}-gtest")

        add_executable(${ARG_TARGET}-gtest ${ARG_TESTS})
        target_include_directories(${ARG_TARGET} PUBLIC src
                ${CMAKE_SOURCE_DIR}/include
                ${BSQL_BLD_PREFIX}/include
                ${BSQL_BLD_PREFIX}/include/cudf
                )
        target_compile_options(${ARG_TARGET} PUBLIC -ggdb -O0)
        target_link_libraries(${ARG_TARGET}-gtest
                ${ARG_TARGET}
                GTest::GTest
                libgmock_main.a
                libgmock.a
                cudf
                )
        add_custom_target(${ARG_TARGET}-gtest-run
                COMMAND $<TARGET_FILE:${ARG_TARGET}-gtest>)

        add_test(NAME ${ARG_TARGET}-gtest
                COMMAND
                ${CMAKE_BINARY_DIR}/${ARG_TARGET}-gtest
                )
        add_dependencies(coverage ${ARG_TARGET}-gtest-run)
    endif ()

endfunction()


if (GTEST_FOUND AND BUILD_TESTS)
    enable_testing()
    add_custom_target(coverage
            COMMAND lcov -c -d ${CMAKE_BINARY_DIR}/src -o coverage.info
            COMMAND lcov -r coverage.info '/usr*' '*boost*' '*build*' -o coverage.info
            COMMAND genhtml coverage.info -o ${CMAKE_BINARY_DIR}/coverage-html)
endif ()

blazingdb_artifact(
    TARGET
        blazingdb-transport

        INCLUDES PUBLIC
        $ENV{PROJECT_SOURCE_DIR}/include
        ${PROJECT_SOURCE_DIR}/include
        /usr/local/cuda/include

    LIBRARIES
        Threads::Threads
        NVCategory
        NVStrings
        rmm
        cudart
        cuda
        zmq
        ${CUDA_CUDA_LIBRARY}
        ${CUDA_NVRTC_LIBRARY}
        ${CUDA_NVTX_LIBRARY}

    PREFIX
        blazingdb

        PUBLIC_HEADERS
        ${CMAKE_CURRENT_SOURCE_DIR}/include/

    SOURCES
        src/blazingdb/transport/Message.cc
        src/blazingdb/transport/Client.cc
        src/blazingdb/transport/Server.cc
        src/blazingdb/transport/MessageQueue.cpp
        src/blazingdb/transport/Address.cc
        src/blazingdb/transport/Node.cc
        src/blazingdb/transport/io/reader_writer.cpp
        src/blazingdb/transport/io/fd_reader_writer.cpp
        src/blazingdb/manager/Context.cc

    TESTS
        # tests/utils/StringInfo.cpp
        tests/utils/Traits/RuntimeTraits.cpp
        
        tests/gpu-tcp-server-client-test.cc
        tests/node-test.cc
)

# Print the project summary
 feature_summary(WHAT ALL INCLUDE_QUIET_PACKAGES FATAL_ON_MISSING_REQUIRED_PACKAGES)

#END MAIN #
