#=============================================================================
# Copyright (c) 2018-2020, NVIDIA CORPORATION.
# Copyright 2018-2019 BlazingDB, Inc.
#     Copyright 2018-2019 Percy Camilo Triveño Aucahuasi <percy@blazingdb.com>
#=============================================================================

cmake_minimum_required(VERSION 3.12 FATAL_ERROR)

project(blazingdb-ral C CXX CUDA)

###################################################################################################
# - compiler options ------------------------------------------------------------------------------

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_C_COMPILER $ENV{CC})
set(CMAKE_CXX_COMPILER $ENV{CXX})
set(CMAKE_CXX_STANDARD_REQUIRED ON)

set(CMAKE_CUDA_STANDARD 17)
set(CMAKE_CUDA_STANDARD_REQUIRED ON)

if(CMAKE_COMPILER_IS_GNUCXX)
    # NOTE felipe percy these flags are too strict for blazingsql
    #set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Werror -Wno-error=deprecated-declarations")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -Wextra")

    # Suppress parentheses warning which causes gmock to fail
    set(CMAKE_CUDA_FLAGS "${CMAKE_CUDA_FLAGS} -Xcompiler -Wno-parentheses")
endif(CMAKE_COMPILER_IS_GNUCXX)

if(CMAKE_CUDA_COMPILER_VERSION)
  # Compute the version. from  CMAKE_CUDA_COMPILER_VERSION
  string(REGEX REPLACE "([0-9]+)\\.([0-9]+).*" "\\1" CUDA_VERSION_MAJOR ${CMAKE_CUDA_COMPILER_VERSION})
  string(REGEX REPLACE "([0-9]+)\\.([0-9]+).*" "\\2" CUDA_VERSION_MINOR ${CMAKE_CUDA_COMPILER_VERSION})
  set(CUDA_VERSION "${CUDA_VERSION_MAJOR}.${CUDA_VERSION_MINOR}" CACHE STRING "Version of CUDA as computed from nvcc.")
  mark_as_advanced(CUDA_VERSION)
endif()

message(STATUS "CUDA_VERSION_MAJOR: ${CUDA_VERSION_MAJOR}")
message(STATUS "CUDA_VERSION_MINOR: ${CUDA_VERSION_MINOR}")
message(STATUS "CUDA_VERSION: ${CUDA_VERSION}")

# Always set this convenience variable
set(CUDA_VERSION_STRING "${CUDA_VERSION}")

# Auto-detect available GPU compute architectures
set(GPU_ARCHS "ALL" CACHE STRING
  "List of GPU architectures (semicolon-separated) to be compiled for. Pass 'ALL' if you want to compile for all supported GPU architectures. Empty string means to auto-detect the GPUs on the current system")

if("${GPU_ARCHS}" STREQUAL "")
  include(cmake/EvalGpuArchs.cmake)
  evaluate_gpu_archs(GPU_ARCHS)
endif()

if("${GPU_ARCHS}" STREQUAL "ALL")
  set(GPU_ARCHS "60")
  if((CUDA_VERSION_MAJOR EQUAL 9) OR (CUDA_VERSION_MAJOR GREATER 9))
    set(GPU_ARCHS "${GPU_ARCHS};70")
  endif()
  if((CUDA_VERSION_MAJOR EQUAL 10) OR (CUDA_VERSION_MAJOR GREATER 10))
    set(GPU_ARCHS "${GPU_ARCHS};75")
  endif()
endif()
message("GPU_ARCHS = ${GPU_ARCHS}")

foreach(arch ${GPU_ARCHS})
  set(CMAKE_CUDA_FLAGS "${CMAKE_CUDA_FLAGS} -gencode=arch=compute_${arch},code=sm_${arch}")
endforeach()

list(GET GPU_ARCHS -1 ptx)
set(CMAKE_CUDA_FLAGS "${CMAKE_CUDA_FLAGS} -gencode=arch=compute_${ptx},code=compute_${ptx}")

set(CMAKE_CUDA_FLAGS "${CMAKE_CUDA_FLAGS} --expt-extended-lambda --expt-relaxed-constexpr")

# set warnings as errors
# TODO: remove `no-maybe-unitialized` used to suppress warnings in rmm::exec_policy
# NOTE felipe percy these flags are too strict for blazingsql: -Werror,
set(CMAKE_CUDA_FLAGS "${CMAKE_CUDA_FLAGS} -Werror=cross-execution-space-call -Xcompiler -Wall,-Wno-error=deprecated-declarations")

option(DISABLE_DEPRECATION_WARNING "Disable warnings generated from deprecated declarations." OFF)
if(DISABLE_DEPRECATION_WARNING)
    set(CMAKE_CUDA_FLAGS "${CMAKE_CUDA_FLAGS} -Xcompiler -Wno-deprecated-declarations")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-deprecated-declarations")
endif(DISABLE_DEPRECATION_WARNING)

# Option to enable line info in CUDA device compilation to allow introspection when profiling / memchecking
option(CMAKE_CUDA_LINEINFO "Enable the -lineinfo option for nvcc (useful for cuda-memcheck / profiler" OFF)
if(CMAKE_CUDA_LINEINFO)
    set(CMAKE_CUDA_FLAGS "${CMAKE_CUDA_FLAGS} -lineinfo")
endif(CMAKE_CUDA_LINEINFO)

# Debug options
if(CMAKE_BUILD_TYPE MATCHES Debug)
    message(STATUS "Building with debugging flags")
    set(CMAKE_CUDA_FLAGS "${CMAKE_CUDA_FLAGS} -G -g -Xcompiler -rdynamic")
endif(CMAKE_BUILD_TYPE MATCHES Debug)

# To apply RUNPATH to transitive dependencies (this is a temporary solution)
set(CMAKE_SHARED_LINKER_FLAGS "-Wl,--disable-new-dtags")
set(CMAKE_EXE_LINKER_FLAGS "-Wl,--disable-new-dtags")

# Build options
option(BUILD_SHARED_LIBS "Build shared libraries" ON)
option(BUILD_TESTS "Configure CMake to build tests" ON)
option(BUILD_BENCHMARKS "Configure CMake to build (google) benchmarks" OFF)

option(GCS_SUPPORT "Enables support for GCS fileSystem" ON)
option(S3_SUPPORT "Enables support for S3 fileSystem" ON)

option(MYSQL_SUPPORT "Enables support for MySQL database" ON)
option(SQLITE_SUPPORT "Enables support for SQLite database" ON)
option(POSTGRESQL_SUPPORT "Enables support for Postgre database" ON)

###################################################################################################
# - cudart options --------------------------------------------------------------------------------
# cudart can be statically linked or dynamically linked. The python ecosystem wants dynamic linking

option(CUDA_STATIC_RUNTIME "Statically link the CUDA runtime" OFF)

if(CUDA_STATIC_RUNTIME)
    message(STATUS "Enabling static linking of cudart")
    set(CUDART_LIBRARY "cudart_static")
else()
    set(CUDART_LIBRARY "cudart")
endif(CUDA_STATIC_RUNTIME)

###################################################################################################

set(CMAKE_MODULE_PATH "${PROJECT_SOURCE_DIR}/cmake/" ${CMAKE_MODULE_PATH})
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
    set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} $ENV{BUILD_PREFIX}/lib64/cmake)
    set(ENV{PKG_CONFIG_PATH} $ENV{BUILD_PREFIX}/lib/pkgconfig/) # used by find libcurl and openssl
    set(ENV{LD_LIBRARY_PATH} $ENV{BUILD_PREFIX}/lib/) # to link the tests correctly against libcurl.so
    link_directories($ENV{BUILD_PREFIX}/lib/)
    link_directories($ENV{BUILD_PREFIX}/lib64/)

elseif (DEFINED ENV{CONDA_PREFIX})
    set(CMAKE_SYSTEM_PREFIX_PATH "$ENV{CONDA_PREFIX};${CMAKE_SYSTEM_PREFIX_PATH}")
    message(STATUS "Conda environment detected, CMAKE_SYSTEM_PREFIX_PATH set to: ${CMAKE_SYSTEM_PREFIX_PATH}")
    set(BSQL_BLD_PREFIX "$ENV{CONDA_PREFIX}")

    set(AWS_SDK_CPP_INSTALL_DIR $ENV{CONDA_PREFIX})
    set(GOOGLE_CLOUD_CPP_INSTALL_DIR $ENV{CONDA_PREFIX})
    set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} $ENV{CONDA_PREFIX}/lib/cmake)
    set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} $ENV{CONDA_PREFIX}/lib64/cmake)
    set(ENV{PKG_CONFIG_PATH} $ENV{CONDA_PREFIX}/lib/pkgconfig/) # used by find libcurl and openssl
    set(ENV{LD_LIBRARY_PATH} $ENV{CONDA_PREFIX}/lib/) # to link the tests correctly against libcurl.so
    link_directories($ENV{CONDA_PREFIX}/lib/)
    link_directories($ENV{CONDA_PREFIX}/lib64/)

endif ()

link_directories(${CMAKE_CUDA_IMPLICIT_LINK_DIRECTORIES})

message(WARNING "BSQL_BLD_PREFIX set to: ${BSQL_BLD_PREFIX}")
message(STATUS "CMAKE_MODULE_PATH: ${CMAKE_MODULE_PATH}")

#########################################################################3

# Include CMake modules
include(FeatureSummary)
include(CheckIncludeFiles)
include(CheckLibraryExists)
include(CTest)

find_package(CURL REQUIRED)

# this breaks powerpc custom compiler
#find_package(Threads)

find_package(PythonLibs)
# TODO jp mario c.gonzales we must use find here!
#find_package(spdlog REQUIRED)

find_package(GTest REQUIRED)

# BEGIN UCX
set(UCX_INSTALL_DIR $ENV{CONDA_PREFIX})
set(UCX_INCLUDE_DIR $ENV{CONDA_PREFIX}/include)
find_package(UCX REQUIRED)
# END UCX

# BEGIN find arrow
find_path(ARROW_INCLUDE_DIR "arrow" HINTS "$ENV{ARROW_ROOT}/include")
find_library(ARROW_LIB "arrow" NAMES libarrow HINTS "$ENV{ARROW_ROOT}/lib" "$ENV{ARROW_ROOT}/build")
find_library(ARROW_CUDA_LIB "arrow_cuda" NAMES libarrow_cuda HINTS "$ENV{ARROW_ROOT}/lib" "$ENV{ARROW_ROOT}/build")
message(STATUS "ARROW: ARROW_INCLUDE_DIR set to ${ARROW_INCLUDE_DIR}")
message(STATUS "ARROW: ARROW_LIB set to ${ARROW_LIB}")
message(STATUS "ARROW: ARROW_CUDA_LIB set to ${ARROW_CUDA_LIB}")
add_library(arrow SHARED IMPORTED ${ARROW_LIB})
add_library(arrow_cuda SHARED IMPORTED ${ARROW_CUDA_LIB})

if(ARROW_INCLUDE_DIR AND ARROW_LIB AND ARROW_CUDA_LIB)
  set_target_properties(arrow PROPERTIES IMPORTED_LOCATION ${ARROW_LIB})
  set_target_properties(arrow_cuda PROPERTIES IMPORTED_LOCATION ${ARROW_CUDA_LIB})
endif(ARROW_INCLUDE_DIR AND ARROW_LIB AND ARROW_CUDA_LIB)
# END find arrow

# BEGIN MAIN #

# TODO percy move this to tool-chain and add support for cuda def and more than 1 defs
add_definitions(${CXX_DEFINES})
get_directory_property(CXX_COMPILE_DEFINITIONS DIRECTORY ${PROJECT_SOURCE_DIR} COMPILE_DEFINITIONS)
message(STATUS "C++ compiler definitions: ${CXX_COMPILE_DEFINITIONS}")


include_directories(
    ${PROJECT_BINARY_DIR}
    ${PROJECT_SOURCE_DIR} ${CMAKE_BINARY_DIR}
    ${BSQL_BLD_PREFIX}/include/libcudf/libcudacxx/
    ${BSQL_BLD_PREFIX}/include
    ${BSQL_BLD_PREFIX}/include/blazingdb/io
    ${PROJECT_SOURCE_DIR}/src
    ${PROJECT_SOURCE_DIR}/include
    ${PROJECT_SOURCE_DIR}/thirdparty/jitify
    ${CMAKE_CUDA_TOOLKIT_INCLUDE_DIRECTORIES}
    ${PYTHON_INCLUDE_DIRS}
    "${ARROW_INCLUDE_DIR}"
    )

if(MYSQL_SUPPORT)
    add_definitions(-DMYSQL_SUPPORT)
    set(MYSQL_DATABASE_SRC_FILES
        # TODO percy in case we need custom mysql transformer
        #${PROJECT_SOURCE_DIR}/src/io/data_provider/sql/compatibility/MySQLTransformer.cpp
        ${PROJECT_SOURCE_DIR}/src/io/data_provider/sql/MySQLDataProvider.cpp
        ${PROJECT_SOURCE_DIR}/src/io/data_parser/sql/MySQLParser.cpp)
    set(MYSQL_LIBRARY libmysqlcppconn-static.a)
    message(STATUS "MySQL support is enabled!")
else()
    message(STATUS "MySQL support is disabled!")
endif()

if(SQLITE_SUPPORT)
    add_definitions(-DSQLITE_SUPPORT)
    set(SQLITE_DATABASE_SRC_FILES
        # TODO percy c.gonzales
        #${PROJECT_SOURCE_DIR}/src/io/data_provider/sql/compatibility/SQLiteTransformer.cpp
        ${PROJECT_SOURCE_DIR}/src/io/data_provider/sql/SQLiteDataProvider.cpp
        ${PROJECT_SOURCE_DIR}/src/io/data_parser/sql/SQLiteParser.cpp)
    set(SQLITE_LIBRARY libsqlite3.so)
    message(STATUS "SQLite support is enabled!")
else()
    message(STATUS "SQLite support is disabled!")
endif()

if(POSTGRESQL_SUPPORT)
    # TODO postgresql percy c.gonzales
    add_definitions(-DPOSTGRESQL_SUPPORT)
    set(POSTGRESQL_DATABASE_SRC_FILES
        # TODO percy c.gonzales
        #${PROJECT_SOURCE_DIR}/src/io/data_provider/sql/compatibility/PostgreSQLTransformer.cpp
        ${PROJECT_SOURCE_DIR}/src/io/data_provider/sql/PostgreSQLDataProvider.cpp
        ${PROJECT_SOURCE_DIR}/src/io/data_parser/sql/PostgreSQLParser.cpp)
    set(POSTGRESQL_LIBRARY libpq.so)
    message(STATUS "PostgreSQL support is enabled!")
else()
    message(STATUS "MyPostgreSQL support is disabled!")
endif()

## Target source files
set(SRC_FILES ${PROJECT_SOURCE_DIR}/src/execution_graph/logic_controllers/BlazingHostTable.cpp
              ${PROJECT_SOURCE_DIR}/src/execution_graph/logic_controllers/CacheData.cpp
              ${PROJECT_SOURCE_DIR}/src/execution_graph/logic_controllers/CacheDataIO.cpp
              ${PROJECT_SOURCE_DIR}/src/execution_graph/logic_controllers/CacheDataLocalFile.cpp
              ${PROJECT_SOURCE_DIR}/src/execution_graph/logic_controllers/ConcatCacheData.cpp
              ${PROJECT_SOURCE_DIR}/src/execution_graph/logic_controllers/CPUCacheData.cpp
              ${PROJECT_SOURCE_DIR}/src/execution_graph/logic_controllers/GPUCacheData.cpp
              ${PROJECT_SOURCE_DIR}/src/execution_graph/logic_controllers/ArrowCacheData.cpp
              ${PROJECT_SOURCE_DIR}/src/execution_graph/logic_controllers/CacheMachine.cpp
              ${PROJECT_SOURCE_DIR}/src/execution_graph/logic_controllers/LogicPrimitives.cpp
              ${PROJECT_SOURCE_DIR}/src/execution_graph/logic_controllers/LogicalFilter.cpp
              ${PROJECT_SOURCE_DIR}/src/execution_graph/logic_controllers/LogicalProject.cpp
              ${PROJECT_SOURCE_DIR}/src/execution_graph/logic_controllers/BlazingColumnOwner.cpp
              ${PROJECT_SOURCE_DIR}/src/execution_graph/logic_controllers/taskflow/port.cpp
              ${PROJECT_SOURCE_DIR}/src/execution_graph/logic_controllers/taskflow/kernel.cpp
              ${PROJECT_SOURCE_DIR}/src/execution_graph/logic_controllers/taskflow/executor.cpp
              ${PROJECT_SOURCE_DIR}/src/execution_graph/logic_controllers/taskflow/distributing_kernel.cpp
              ${PROJECT_SOURCE_DIR}/src/execution_graph/logic_controllers/taskflow/kernel_type.cpp
              ${PROJECT_SOURCE_DIR}/src/execution_graph/Context.cpp
              ${PROJECT_SOURCE_DIR}/src/bmr/MemoryMonitor.cpp
              ${PROJECT_SOURCE_DIR}/src/bmr/BufferProvider.cpp
              ${PROJECT_SOURCE_DIR}/src/bmr/BlazingMemoryResource.cpp
              ${PROJECT_SOURCE_DIR}/src/execution_graph/logic_controllers/taskflow/graph.cpp
              ${PROJECT_SOURCE_DIR}/src/execution_graph/logic_controllers/BatchAggregationProcessing.cpp
              ${PROJECT_SOURCE_DIR}/src/execution_graph/logic_controllers/BatchJoinProcessing.cpp
              ${PROJECT_SOURCE_DIR}/src/execution_graph/logic_controllers/BatchOrderByProcessing.cpp
              ${PROJECT_SOURCE_DIR}/src/execution_graph/logic_controllers/BatchUnionProcessing.cpp
              ${PROJECT_SOURCE_DIR}/src/execution_graph/logic_controllers/BatchWindowFunctionProcessing.cpp
              ${PROJECT_SOURCE_DIR}/src/execution_graph/logic_controllers/BatchProcessing.cpp
              ${PROJECT_SOURCE_DIR}/src/config/GPUManager.cu
              ${PROJECT_SOURCE_DIR}/src/operators/OrderBy.cpp
              ${PROJECT_SOURCE_DIR}/src/operators/GroupBy.cpp
              ${PROJECT_SOURCE_DIR}/src/io/data_provider/sql/compatibility/SQLTranspiler.cpp
              ${PROJECT_SOURCE_DIR}/src/io/data_provider/sql/AbstractSQLDataProvider.cpp
              ${PROJECT_SOURCE_DIR}/src/io/data_provider/UriDataProvider.cpp
              ${PROJECT_SOURCE_DIR}/src/io/data_provider/GDFDataProvider.cpp
              ${PROJECT_SOURCE_DIR}/src/io/data_provider/ArrowDataProvider.cpp
              ${PROJECT_SOURCE_DIR}/src/io/Schema.cpp
              ${PROJECT_SOURCE_DIR}/src/io/data_parser/sql/AbstractSQLParser.cpp
              ${PROJECT_SOURCE_DIR}/src/io/data_parser/ParquetParser.cpp
              ${PROJECT_SOURCE_DIR}/src/io/data_parser/CSVParser.cpp
              ${PROJECT_SOURCE_DIR}/src/io/data_parser/JSONParser.cpp
              ${PROJECT_SOURCE_DIR}/src/io/data_parser/GDFParser.cpp
              ${PROJECT_SOURCE_DIR}/src/io/data_parser/OrcParser.cpp
              ${PROJECT_SOURCE_DIR}/src/io/data_parser/ArrowParser.cpp
              ${PROJECT_SOURCE_DIR}/src/io/data_parser/ArgsUtil.cpp
              ${PROJECT_SOURCE_DIR}/src/io/data_parser/metadata/parquet_metadata.cpp
              ${PROJECT_SOURCE_DIR}/src/io/data_parser/metadata/orc_metadata.cpp
              ${PROJECT_SOURCE_DIR}/src/io/data_parser/metadata/common_metadata.cpp
              ${PROJECT_SOURCE_DIR}/src/utilities/CommonOperations.cpp
              ${PROJECT_SOURCE_DIR}/src/utilities/scalar_timestamp_parser.cpp
              ${PROJECT_SOURCE_DIR}/src/utilities/DebuggingUtils.cpp
              ${PROJECT_SOURCE_DIR}/src/utilities/transform.cu
              ${PROJECT_SOURCE_DIR}/src/CalciteExpressionParsing.cpp
              ${PROJECT_SOURCE_DIR}/src/io/DataLoader.cpp
              ${PROJECT_SOURCE_DIR}/src/Interpreter/interpreter_cpp.cu
              ${PROJECT_SOURCE_DIR}/src/CalciteInterpreter.cpp
              ${PROJECT_SOURCE_DIR}/src/parser/expression_utils.cpp
              ${PROJECT_SOURCE_DIR}/src/parser/expression_tree.cpp
              ${PROJECT_SOURCE_DIR}/src/skip_data/SkipDataProcessor.cpp
              ${PROJECT_SOURCE_DIR}/src/skip_data/utils.cpp
              ${PROJECT_SOURCE_DIR}/src/cython/static.cpp
              ${PROJECT_SOURCE_DIR}/src/cython/initialize.cpp
              ${PROJECT_SOURCE_DIR}/src/cython/io.cpp
              ${PROJECT_SOURCE_DIR}/src/cython/errors.cpp
              ${PROJECT_SOURCE_DIR}/src/cython/engine.cpp
              ${PROJECT_SOURCE_DIR}/src/distribution/primitives.cpp

              ${PROJECT_SOURCE_DIR}/src/communication/factory/MessageFactory.cpp
              ${PROJECT_SOURCE_DIR}/src/communication/CommunicationData.cpp
              ${PROJECT_SOURCE_DIR}/src/communication/messages/MessageUtil.cu
              ${PROJECT_SOURCE_DIR}/src/communication/messages/GPUComponentMessage.cpp
              ${PROJECT_SOURCE_DIR}/src/communication/CommunicationInterface/serializer.cpp
              ${PROJECT_SOURCE_DIR}/src/communication/CommunicationInterface/node.cpp
              ${PROJECT_SOURCE_DIR}/src/communication/CommunicationInterface/messageReceiver.cpp
              ${PROJECT_SOURCE_DIR}/src/communication/CommunicationInterface/messageListener.cpp
              ${PROJECT_SOURCE_DIR}/src/communication/CommunicationInterface/bufferTransport.cpp
              ${PROJECT_SOURCE_DIR}/src/communication/CommunicationInterface/protocols.cpp
              ${PROJECT_SOURCE_DIR}/src/communication/CommunicationInterface/messageSender.cpp

              ${PROJECT_SOURCE_DIR}/src/transport/Node.cpp

              # SQL
              ${MYSQL_DATABASE_SRC_FILES}
              ${SQLITE_DATABASE_SRC_FILES}
              ${POSTGRESQL_DATABASE_SRC_FILES}
        )

link_directories(${BSQL_BLD_PREFIX}/lib)

# These new var are for custom build process (powerpc)
link_directories($ENV{SNAPPY_INSTALL_DIR})
link_directories($ENV{LZ4_INSTALL_DIR})

add_library(blazingsql-engine SHARED
    ${SRC_FILES}
    ${MYSQL_DATABASE_SRC_FILES}
    ${SQLITE_DATABASE_SRC_FILES}
    ${POSTGRESQL_DATABASE_SRC_FILES}
)

if(GCS_SUPPORT)
    find_package(storage_client REQUIRED)
    set(GCS_LIBRARY storage_client google_cloud_cpp_common crc32c)
endif()

if(S3_SUPPORT)
    set(S3_LIBRARY aws-cpp-sdk-s3 aws-cpp-sdk-s3-encryption)
endif()

set_target_properties(blazingsql-engine PROPERTIES POSITION_INDEPENDENT_CODE ON)
target_link_libraries(blazingsql-engine
    blazingdb-io
    #Threads::Threads
    pthread

    cudf
    zmq
    cudart

    ${UCX_LIBRARIES}

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

    # TODO jp c.gonzales make use of standard cmake find_package here (see above)
    #spdlog::spdlog
    libspdlog.a

    # Library needed for blazing communication repository (debug)
    gcov

    # TODO percy use cmake::find
    ${MYSQL_LIBRARY}
    ${SQLITE_LIBRARY}
    ${POSTGRESQL_LIBRARY}
)

if(CMAKE_BUILD_TYPE STREQUAL "Release" OR CMAKE_BUILD_TYPE STREQUAL "RelWithDebInfo" OR "$ENV{CONDA_BUILD}" STREQUAL "1")
    message(STATUS "This is a release build so the final runtime binaries will not depend on gtest")
else()
    message(WARNING "This is not a release build so the final runtime binaries will depend on gtest (BSQLDBGUTILS precompiler definition is set)")
    add_definitions(-DBSQLDBGUTILS) # see DebuggingUtils.cpp
    target_link_libraries(blazingsql-engine
        gtest
        cudftestutil
    )
endif()

###################################################################################################
# - build options ---------------------------------------------------------------------------------
set(CMAKE_CUDA_FLAGS "${CMAKE_CUDA_FLAGS} --default-stream=per-thread")

option(USE_NVTX "Build with NVTX support" ON)
if(USE_NVTX)
    message(STATUS "Using Nvidia Tools Extension")
    # The `USE_NVTX` macro is deprecated
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DUSE_NVTX")
else()
    # Makes NVTX APIs no-ops
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DDISABLE_NVTX")
endif(USE_NVTX)

option(HT_DEFAULT_ALLOCATOR "Use the default allocator for hash tables" ON)
if(HT_DEFAULT_ALLOCATOR)
    message(STATUS "Using default allocator for hash tables")
    set(CMAKE_CUDA_FLAGS "${CMAKE_CUDA_FLAGS} -DHT_DEFAULT_ALLOCATOR")
endif(HT_DEFAULT_ALLOCATOR)

###################################################################################################

#install(TARGETS testing-libgdf_lib DESTINATION lib RENAME libblazingsql-engine.a)
install(FILES ${CMAKE_CURRENT_BINARY_DIR}/libblazingsql-engine.so DESTINATION lib RENAME libblazingsql-engine.so)

# Tests
if(BUILD_TESTING)
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


include(ConfigureProductVersion)
message(STATUS "cmake_cuda_flags = ${CMAKE_CUDA_FLAGS}")

# Print the project summary
feature_summary(WHAT ALL INCLUDE_QUIET_PACKAGES FATAL_ON_MISSING_REQUIRED_PACKAGES)

#END MAIN #
