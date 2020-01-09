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

if ("$ENV{CONDA_BUILD}" STREQUAL "1")
    set(CMAKE_SYSTEM_PREFIX_PATH "$ENV{BUILD_PREFIX};$ENV{PREFIX};${CMAKE_SYSTEM_PREFIX_PATH}")
    message(STATUS "Conda build detected, CMAKE_SYSTEM_PREFIX_PATH set to: ${CMAKE_SYSTEM_PREFIX_PATH}")

    set(AWS_SDK_CPP_INSTALL_DIR $ENV{BUILD_PREFIX})
    set(GOOGLE_CLOUD_CPP_INSTALL_DIR $ENV{BUILD_PREFIX})
    set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} $ENV{BUILD_PREFIX}/lib/cmake)
    set(ENV{PKG_CONFIG_PATH} $ENV{BUILD_PREFIX}/lib/pkgconfig/) # used by find libcurl and openssl
    set(ENV{LD_LIBRARY_PATH} $ENV{BUILD_PREFIX}/lib/) # to link the tests correctly against libcurl.so

elseif (DEFINED ENV{CONDA_PREFIX})
    set(CMAKE_SYSTEM_PREFIX_PATH "$ENV{CONDA_PREFIX};${CMAKE_SYSTEM_PREFIX_PATH}")
    message(STATUS "Conda environment detected, CMAKE_SYSTEM_PREFIX_PATH set to: ${CMAKE_SYSTEM_PREFIX_PATH}")

    set(AWS_SDK_CPP_INSTALL_DIR $ENV{CONDA_PREFIX})
    set(GOOGLE_CLOUD_CPP_INSTALL_DIR $ENV{CONDA_PREFIX})
    set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} $ENV{CONDA_PREFIX}/lib/cmake)
    set(ENV{PKG_CONFIG_PATH} $ENV{CONDA_PREFIX}/lib/pkgconfig/) # used by find libcurl and openssl
    set(ENV{LD_LIBRARY_PATH} $ENV{CONDA_PREFIX}/lib/) # to link the tests correctly against libcurl.so

endif ()

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
    ${CMAKE_SOURCE_DIR}/src/gdf_wrapper
    ${CMAKE_SOURCE_DIR}/src/gdf_wrapper
    $ENV{CONDA_PREFIX}/include
    $ENV{CONDA_PREFIX}/include/cudf
	$ENV{CONDA_PREFIX}/include/cub
	$ENV{CONDA_PREFIX}/include/libcudacxx/include
    $ENV{CONDA_PREFIX}/include/blazingdb/io
    $ENV{PREFIX}/include
    $ENV{PREFIX}/include/cudf
	$ENV{PREFIX}/include/cub
	$ENV{PREFIX}/include/libcudacxx/include
    $ENV{PREFIX}/include/blazingdb/io
    ${PYTHON_INCLUDE_DIRS}
    )

## cuDF adapter source files
add_subdirectory(${CMAKE_SOURCE_DIR}/src/cuDF)
get_directory_property(cudf_adapter_source_files DIRECTORY ${CMAKE_SOURCE_DIR}/src/cuDF DEFINITION source_files)

## communication primitives
add_subdirectory(${CMAKE_SOURCE_DIR}/src/communication)
get_directory_property(communication_source_files DIRECTORY ${CMAKE_SOURCE_DIR}/src/communication DEFINITION source_files)

## communication primitives
add_subdirectory(${CMAKE_SOURCE_DIR}/src/distribution)
get_directory_property(distribution_source_files DIRECTORY ${CMAKE_SOURCE_DIR}/src/distribution DEFINITION source_files)

## Target source files
set(SRC_FILES ${CMAKE_SOURCE_DIR}/src/config/BlazingConfig.cpp
              ${CMAKE_SOURCE_DIR}/src/config/GPUManager.cu
              ${CMAKE_SOURCE_DIR}/src/exception/RalException.cpp
              ${CMAKE_SOURCE_DIR}/src/operators/OrderBy.cpp
              ${CMAKE_SOURCE_DIR}/src/operators/JoinOperator.cpp
              ${CMAKE_SOURCE_DIR}/src/operators/GroupBy.cpp
              ${CMAKE_SOURCE_DIR}/src/io/data_provider/UriDataProvider.cpp
              ${CMAKE_SOURCE_DIR}/src/io/Schema.cpp
              ${CMAKE_SOURCE_DIR}/src/io/data_parser/ParquetParser.cpp
              ${CMAKE_SOURCE_DIR}/src/io/data_parser/CSVParser.cpp
              ${CMAKE_SOURCE_DIR}/src/io/data_parser/JSONParser.cpp
              ${CMAKE_SOURCE_DIR}/src/io/data_parser/GDFParser.cpp
              ${CMAKE_SOURCE_DIR}/src/io/data_parser/OrcParser.cpp
              ${CMAKE_SOURCE_DIR}/src/io/data_parser/ArrowParser.cpp
              ${CMAKE_SOURCE_DIR}/src/io/data_parser/ParserUtil.cpp
              ${CMAKE_SOURCE_DIR}/src/io/data_parser/ArgsUtil.cpp
              ${CMAKE_SOURCE_DIR}/src/io/data_parser/metadata/parquet_metadata.cpp
              ${CMAKE_SOURCE_DIR}/src/Traits/RuntimeTraits.cpp
              ${CMAKE_SOURCE_DIR}/src/utilities/RalColumn.cpp
              ${CMAKE_SOURCE_DIR}/src/utilities/CommonOperations.cpp
              ${CMAKE_SOURCE_DIR}/src/utilities/TableWrapper.cpp
              ${CMAKE_SOURCE_DIR}/src/utilities/StringUtils.cpp
              ${CMAKE_CURRENT_SOURCE_DIR}/src/Config/Config.cpp
              ${CMAKE_SOURCE_DIR}/src/CalciteExpressionParsing.cpp
              ${CMAKE_SOURCE_DIR}/src/io/DataLoader.cpp
              ${CMAKE_SOURCE_DIR}/src/Interpreter/interpreter_cpp.cu
              ${CMAKE_SOURCE_DIR}/src/CalciteInterpreter.cpp
              ${CMAKE_SOURCE_DIR}/src/ColumnManipulation.cu
              ${CMAKE_SOURCE_DIR}/src/ResultSetRepository.cpp
              ${CMAKE_SOURCE_DIR}/src/JoinProcessor.cpp
              ${CMAKE_SOURCE_DIR}/src/LogicalFilter.cpp
              #${CMAKE_SOURCE_DIR}/src/QueryState.cpp
              ${CMAKE_SOURCE_DIR}/src/CodeTimer.cpp
              ${CMAKE_SOURCE_DIR}/src/Utils.cu
			  ${CMAKE_SOURCE_DIR}/src/from_cudf/cpp_tests/utilities/column_utilities.cu
              ${CMAKE_SOURCE_DIR}/src/GDFCounter.cu
              ${CMAKE_SOURCE_DIR}/src/GDFColumn.cu
              ${CMAKE_SOURCE_DIR}/src/parser/expression_utils.cpp
              ${CMAKE_SOURCE_DIR}/src/execution_graph/logic_controllers/LogicPrimitives.cpp
              ${CMAKE_SOURCE_DIR}/src/execution_graph/logic_controllers/LogicalFilter.cpp
              ${CMAKE_SOURCE_DIR}/src/skip_data/SkipDataProcessor.cpp
              ${CMAKE_SOURCE_DIR}/src/skip_data/utils.cpp
              ${CMAKE_SOURCE_DIR}/src/skip_data/expression_tree.cpp
              ${CMAKE_SOURCE_DIR}/src/cython/initialize.cpp
              ${CMAKE_SOURCE_DIR}/src/cython/io.cpp
              ${CMAKE_SOURCE_DIR}/src/cython/errors.cpp
              ${CMAKE_SOURCE_DIR}/src/cython/engine.cpp
              ${cudf_adapter_source_files}
              ${binary_operation_source_files}
              ${communication_source_files}
              ${distribution_source_files}
)

link_directories($ENV{CONDA_PREFIX}/lib)

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
