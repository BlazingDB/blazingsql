#=============================================================================
# Copyright 2018-2019 BlazingDB, Inc.
#     Copyright 2018-2019 Percy Camilo Triveño Aucahuasi <percy@blazingdb.com>
#=============================================================================

cmake_minimum_required(VERSION 3.12 FATAL_ERROR)

project(blazingdb-ral C CXX CUDA)

set(CMAKE_MODULE_PATH "${CMAKE_SOURCE_DIR}/cmake/" ${CMAKE_MODULE_PATH})
message(STATUS "CMAKE_MODULE_PATH: ${CMAKE_MODULE_PATH}")

###################################################################################################
# - conda environment -----------------------------------------------------------------------------

set(BSQL_BLD_PREFIX "")
if ("$ENV{CONDA_BUILD}" STREQUAL "1")
    set(CMAKE_SYSTEM_PREFIX_PATH "$ENV{BUILD_PREFIX};$ENV{PREFIX};${CMAKE_SYSTEM_PREFIX_PATH}")
    message(STATUS "Conda build detected, CMAKE_SYSTEM_PREFIX_PATH set to: ${CMAKE_SYSTEM_PREFIX_PATH}")
    set(BSQL_BLD_PREFIX "$ENV{BUILD_PREFIX}")

    set(AWS_SDK_CPP_INSTALL_DIR $ENV{BUILD_PREFIX})
    set(GOOGLE_CLOUD_CPP_INSTALL_DIR $ENV{BUILD_PREFIX})
    set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} $ENV{BUILD_PREFIX}/lib/cmake)
    set(ENV{PKG_CONFIG_PATH} $ENV{BUILD_PREFIX}/lib/pkgconfig/) # used by find libcurl and openssl
    set(ENV{LD_LIBRARY_PATH} $ENV{BUILD_PREFIX}/lib/) # to link the tests correctly against libcurl.so

elseif (DEFINED ENV{CONDA_PREFIX})
    set(CMAKE_SYSTEM_PREFIX_PATH "$ENV{CONDA_PREFIX};${CMAKE_SYSTEM_PREFIX_PATH}")
    message(STATUS "Conda environment detected, CMAKE_SYSTEM_PREFIX_PATH set to: ${CMAKE_SYSTEM_PREFIX_PATH}")
    set(BSQL_BLD_PREFIX "$ENV{CONDA_PREFIX}")

    set(AWS_SDK_CPP_INSTALL_DIR $ENV{CONDA_PREFIX})
    set(GOOGLE_CLOUD_CPP_INSTALL_DIR $ENV{CONDA_PREFIX})
    set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} $ENV{CONDA_PREFIX}/lib/cmake)
    set(ENV{PKG_CONFIG_PATH} $ENV{CONDA_PREFIX}/lib/pkgconfig/) # used by find libcurl and openssl
    set(ENV{LD_LIBRARY_PATH} $ENV{CONDA_PREFIX}/lib/) # to link the tests correctly against libcurl.so

endif ()
message(WARNING "BSQL_BLD_PREFIX set to: ${BSQL_BLD_PREFIX}")

#########################################################################3
# - compiler stuff ------------------
set(CMAKE_CXX_STANDARD 14)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

set(CMAKE_CUDA_STANDARD 14)
set(CMAKE_CUDA_STANDARD_REQURIED ON)

# Include CMake modules
include(FeatureSummary)
include(CheckIncludeFiles)
include(CheckLibraryExists)
include(CTest)

# Configure CUDA
include(ConfigureCUDA)

# NOTE Do not change the inclusion order
include(ConfigureAWSSDK)
include(ConfigureGoogleCloudCPP)

find_package(Threads)
find_package(PythonLibs)

# BEGIN MAIN #

# TODO percy move this to tool-chain and add support for cuda def and more than 1 defs
add_definitions(${CXX_DEFINES})
get_directory_property(CXX_COMPILE_DEFINITIONS DIRECTORY ${CMAKE_SOURCE_DIR} COMPILE_DEFINITIONS)
message(STATUS "C++ compiler definitions: ${CXX_COMPILE_DEFINITIONS}")

include_directories(
    ${CMAKE_SOURCE_DIR} ${CMAKE_BINARY_DIR}
    ${CMAKE_CUDA_TOOLKIT_INCLUDE_DIRECTORIES}
    ${CMAKE_SOURCE_DIR}/src
    ${CMAKE_SOURCE_DIR}/include
    ${CMAKE_SOURCE_DIR}/thirdparty/jitify
    ${BSQL_BLD_PREFIX}/include
    ${BSQL_BLD_PREFIX}/include/cudf
    ${BSQL_BLD_PREFIX}/include/bsql-rapids-thirdparty/cub
    ${BSQL_BLD_PREFIX}/include/bsql-rapids-thirdparty/libcudacxx/include
    ${BSQL_BLD_PREFIX}/include/blazingdb/io
    ${PREFIX}/include
    ${PREFIX}/include/cudf
    ${PREFIX}/include/bsql-rapids-thirdparty/cub
    ${PREFIX}/include/bsql-rapids-thirdparty/libcudacxx/include
    ${PREFIX}/include/blazingdb/io
    ${CMAKE_INSTALL_PREFIX}/include
    ${CMAKE_INSTALL_PREFIX}/include/cudf
    ${CMAKE_INSTALL_PREFIX}/include/bsql-rapids-thirdparty/cub
    ${CMAKE_INSTALL_PREFIX}/include/bsql-rapids-thirdparty/libcudacxx/include
    ${CMAKE_INSTALL_PREFIX}/include/blazingdb/io
    $ENV{INSTALL_PREFIX}/include
    $ENV{INSTALL_PREFIX}/include/cudf
    $ENV{INSTALL_PREFIX}/include/bsql-rapids-thirdparty/cub
    $ENV{INSTALL_PREFIX}/include/bsql-rapids-thirdparty/libcudacxx/include
    $ENV{INSTALL_PREFIX}/include/blazingdb/io
    ${PYTHON_INCLUDE_DIRS}
    )

## communication primitives
add_subdirectory(${CMAKE_SOURCE_DIR}/src/communication)
get_directory_property(communication_source_files DIRECTORY ${CMAKE_SOURCE_DIR}/src/communication DEFINITION source_files)

## Target source files
set(SRC_FILES ${CMAKE_SOURCE_DIR}/src/config/BlazingConfig.cpp
              ${CMAKE_SOURCE_DIR}/src/config/GPUManager.cu
              ${CMAKE_SOURCE_DIR}/src/operators/OrderBy.cpp
              ${CMAKE_SOURCE_DIR}/src/operators/GroupBy.cpp
              ${CMAKE_SOURCE_DIR}/src/io/data_provider/UriDataProvider.cpp
              ${CMAKE_SOURCE_DIR}/src/io/Schema.cpp
              ${CMAKE_SOURCE_DIR}/src/io/data_parser/ParquetParser.cpp
              ${CMAKE_SOURCE_DIR}/src/io/data_parser/CSVParser.cpp
              ${CMAKE_SOURCE_DIR}/src/io/data_parser/JSONParser.cpp
              ${CMAKE_SOURCE_DIR}/src/io/data_parser/GDFParser.cpp
              ${CMAKE_SOURCE_DIR}/src/io/data_parser/OrcParser.cpp
              ${CMAKE_SOURCE_DIR}/src/io/data_parser/ArrowParser.cpp
              ${CMAKE_SOURCE_DIR}/src/io/data_parser/ArgsUtil.cpp
              ${CMAKE_SOURCE_DIR}/src/io/data_parser/metadata/parquet_metadata.cpp
              ${CMAKE_SOURCE_DIR}/src/Traits/RuntimeTraits.cpp
              ${CMAKE_SOURCE_DIR}/src/utilities/CommonOperations.cpp
              ${CMAKE_SOURCE_DIR}/src/utilities/StringUtils.cpp
              ${CMAKE_SOURCE_DIR}/src/utilities/scalar_timestamp_parser.cpp
              ${CMAKE_SOURCE_DIR}/src/utilities/DebuggingUtils.cpp
              ${CMAKE_SOURCE_DIR}/src/utilities/random_generator.cu
              ${CMAKE_SOURCE_DIR}/src/CalciteExpressionParsing.cpp
              ${CMAKE_SOURCE_DIR}/src/io/DataLoader.cpp
              ${CMAKE_SOURCE_DIR}/src/Interpreter/interpreter_cpp.cu
              ${CMAKE_SOURCE_DIR}/src/CalciteInterpreter.cpp
              ${CMAKE_SOURCE_DIR}/src/CodeTimer.cpp
              ${CMAKE_SOURCE_DIR}/src/parser/expression_utils.cpp
              ${CMAKE_SOURCE_DIR}/src/execution_graph/logic_controllers/CacheMachine.cpp
              ${CMAKE_SOURCE_DIR}/src/execution_graph/logic_controllers/LogicPrimitives.cpp
              ${CMAKE_SOURCE_DIR}/src/execution_graph/logic_controllers/LogicalFilter.cpp
              ${CMAKE_SOURCE_DIR}/src/execution_graph/logic_controllers/LogicalProject.cpp
              ${CMAKE_SOURCE_DIR}/src/execution_graph/logic_controllers/BlazingColumnOwner.cpp
              ${CMAKE_SOURCE_DIR}/src/execution_graph/logic_controllers/TableScan.cpp
              ${CMAKE_SOURCE_DIR}/src/execution_graph/logic_controllers/BlazingThread.cpp
              ${CMAKE_SOURCE_DIR}/src/execution_graph/logic_controllers/BlazingExceptionHolder.cpp
              ${CMAKE_SOURCE_DIR}/src/execution_graph/logic_controllers/TaskFlowProcessor.cpp
              ${CMAKE_SOURCE_DIR}/src/skip_data/SkipDataProcessor.cpp
              ${CMAKE_SOURCE_DIR}/src/skip_data/utils.cpp
              ${CMAKE_SOURCE_DIR}/src/skip_data/expression_tree.cpp
              ${CMAKE_SOURCE_DIR}/src/cython/initialize.cpp
              ${CMAKE_SOURCE_DIR}/src/cython/io.cpp
              ${CMAKE_SOURCE_DIR}/src/cython/errors.cpp
              ${CMAKE_SOURCE_DIR}/src/cython/engine.cpp
              ${CMAKE_SOURCE_DIR}/src/from_cudf/cpp_tests/utilities/column_utilities.cu
              ${CMAKE_SOURCE_DIR}/src/distribution/primitives.cpp
              ${communication_source_files}
)

link_directories(${BSQL_BLD_PREFIX}/lib)

add_library(blazingsql-engine SHARED ${SRC_FILES})

set_target_properties(blazingsql-engine PROPERTIES POSITION_INDEPENDENT_CODE ON)
target_link_libraries(blazingsql-engine
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

    # Library needed for blazing communication repository (debug)
    gcov

    gtest
)

#install(TARGETS testing-libgdf_lib DESTINATION lib RENAME libblazingsql-engine.a)
install(FILES ${CMAKE_CURRENT_BINARY_DIR}/libblazingsql-engine.so DESTINATION lib RENAME libblazingsql-engine.so)

# Tests
if(BUILD_TESTING)
    find_package(GTest REQUIRED)

    if(GTEST_FOUND)
        add_subdirectory(tests)
    else()
        message(AUTHOR_WARNING "Google C++ Testing Framework (Google Test) not found: automated tests are disabled.")
    endif()
endif()

#Benchmarks
if(GBENCHMARK_FOUND)
    add_subdirectory(benchmarks)
else()
    message(AUTHOR_WARNING "Google C++ Benchmarking Framework (Google Benchmark) not found")
endif()

# Print the project summary
feature_summary(WHAT ALL INCLUDE_QUIET_PACKAGES FATAL_ON_MISSING_REQUIRED_PACKAGES)

#END MAIN #
