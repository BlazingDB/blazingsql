#=============================================================================
# Copyright (c) 2018-2020, NVIDIA CORPORATION.
# Copyright 2018-2019 BlazingDB, Inc.
#     Copyright 2018-2019 Percy Camilo Triveño Aucahuasi <percy@blazingdb.com>
#=============================================================================

cmake_minimum_required(VERSION 3.12 FATAL_ERROR)

project(blazingdb-ral C CXX CUDA)

###################################################################################################
# - compiler options ------------------------------------------------------------------------------

set(CMAKE_CXX_STANDARD 14)
set(CMAKE_C_COMPILER $ENV{CC})
set(CMAKE_CXX_COMPILER $ENV{CXX})
set(CMAKE_CXX_STANDARD_REQUIRED ON)

set(CMAKE_CUDA_STANDARD 14)
set(CMAKE_CUDA_STANDARD_REQUIRED ON)

if(CMAKE_COMPILER_IS_GNUCXX)
    # NOTE felipe percy these flags are too strict for blazingsql
    #set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Werror -Wno-error=deprecated-declarations")

    # Suppress parentheses warning which causes gmock to fail
    set(CMAKE_CUDA_FLAGS "${CMAKE_CUDA_FLAGS} -Xcompiler -Wno-parentheses")

    option(CMAKE_CXX11_ABI "Enable the GLIBCXX11 ABI" ON)
    if(CMAKE_CXX11_ABI)
        message(STATUS "CUDF: Enabling the GLIBCXX11 ABI")
    else()
        message(STATUS "CUDF: Disabling the GLIBCXX11 ABI")
        set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -D_GLIBCXX_USE_CXX11_ABI=0")
        set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -D_GLIBCXX_USE_CXX11_ABI=0")
        set(CMAKE_CUDA_FLAGS "${CMAKE_CUDA_FLAGS} -Xcompiler -D_GLIBCXX_USE_CXX11_ABI=0")
    endif(CMAKE_CXX11_ABI)
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

find_library(CUDFTESTUTIL_LIBRARY "libcudftestutil.a"
             HINTS "$ENV{CUDF_ROOT}/lib")

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
message(WARNING "BSQL_BLD_PREFIX set to: ${BSQL_BLD_PREFIX}")
message(STATUS "CMAKE_MODULE_PATH: ${CMAKE_MODULE_PATH}")

#########################################################################3

# Include CMake modules
include(FeatureSummary)
include(CheckIncludeFiles)
include(CheckLibraryExists)
include(CTest)

find_package(CURL REQUIRED)

find_package(Threads)
find_package(PythonLibs)
# TODO jp mario c.gonzales we must use find here!
#find_package(spdlog REQUIRED)

find_package(GTest REQUIRED)

set(UCX_INSTALL_DIR $ENV{CONDA_PREFIX})
set(UCX_INCLUDE_DIR $ENV{CONDA_PREFIX}/include)
find_package(UCX REQUIRED)

# BEGIN MAIN #

# TODO percy move this to tool-chain and add support for cuda def and more than 1 defs
add_definitions(${CXX_DEFINES})
get_directory_property(CXX_COMPILE_DEFINITIONS DIRECTORY ${CMAKE_SOURCE_DIR} COMPILE_DEFINITIONS)
message(STATUS "C++ compiler definitions: ${CXX_COMPILE_DEFINITIONS}")


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
    ${PROJECT_BINARY_DIR}
    ${CMAKE_SOURCE_DIR} ${CMAKE_BINARY_DIR}
    ${CMAKE_CUDA_TOOLKIT_INCLUDE_DIRECTORIES}
    ${CMAKE_SOURCE_DIR}/src
    ${CMAKE_SOURCE_DIR}/include
    ${CMAKE_SOURCE_DIR}/thirdparty/jitify
    ${BSQL_BLD_PREFIX}/include
    ${BSQL_BLD_PREFIX}/include/cudf
    ${BSQL_BLD_PREFIX}/include/libcudf/libcudacxx/
    ${BSQL_BLD_PREFIX}/include/blazingdb/io
    ${PREFIX}/include
    ${PREFIX}/include/cudf
    ${PREFIX}/include/libcudf/libcudacxx/
    ${PREFIX}/include/blazingdb/io
    ${CMAKE_INSTALL_PREFIX}/include
    ${CMAKE_INSTALL_PREFIX}/include/cudf
    ${CMAKE_INSTALL_PREFIX}/include/libcudf/libcudacxx/
    ${CMAKE_INSTALL_PREFIX}/include/blazingdb/io
    $ENV{INSTALL_PREFIX}/include
    $ENV{INSTALL_PREFIX}/include/cudf
    $ENV{INSTALL_PREFIX}/include/libcudf/libcudacxx/
    $ENV{INSTALL_PREFIX}/include/blazingdb/io
    ${PYTHON_INCLUDE_DIRS}
    ${CUDF_TEST_INCLUDE}
    )

## Target source files
set(SRC_FILES ${CMAKE_SOURCE_DIR}/src/execution_graph/logic_controllers/BlazingHostTable.cpp
              ${CMAKE_SOURCE_DIR}/src/execution_graph/logic_controllers/CacheMachine.cpp
              ${CMAKE_SOURCE_DIR}/src/execution_graph/logic_controllers/LogicPrimitives.cpp
              ${CMAKE_SOURCE_DIR}/src/execution_graph/logic_controllers/LogicalFilter.cpp
              ${CMAKE_SOURCE_DIR}/src/execution_graph/logic_controllers/LogicalProject.cpp
              ${CMAKE_SOURCE_DIR}/src/execution_graph/logic_controllers/BlazingColumnOwner.cpp
              ${CMAKE_SOURCE_DIR}/src/execution_graph/logic_controllers/taskflow/port.cpp
              ${CMAKE_SOURCE_DIR}/src/execution_graph/logic_controllers/taskflow/kernel.cpp
              ${CMAKE_SOURCE_DIR}/src/execution_graph/logic_controllers/taskflow/kernel_type.cpp
              ${CMAKE_SOURCE_DIR}/src/bmr/MemoryMonitor.cpp
              ${CMAKE_SOURCE_DIR}/src/execution_graph/logic_controllers/taskflow/graph.cpp
              ${CMAKE_SOURCE_DIR}/src/execution_graph/logic_controllers/BatchJoinProcessing.cpp
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
              ${CMAKE_SOURCE_DIR}/src/utilities/CommonOperations.cpp
              ${CMAKE_SOURCE_DIR}/src/utilities/StringUtils.cpp
              ${CMAKE_SOURCE_DIR}/src/utilities/scalar_timestamp_parser.cpp
              ${CMAKE_SOURCE_DIR}/src/utilities/DebuggingUtils.cpp
              ${CMAKE_SOURCE_DIR}/src/utilities/transform.cu
              ${CMAKE_SOURCE_DIR}/src/CalciteExpressionParsing.cpp
              ${CMAKE_SOURCE_DIR}/src/io/DataLoader.cpp
              ${CMAKE_SOURCE_DIR}/src/Interpreter/interpreter_cpp.cu
              ${CMAKE_SOURCE_DIR}/src/CalciteInterpreter.cpp
              ${CMAKE_SOURCE_DIR}/src/parser/expression_utils.cpp
              ${CMAKE_SOURCE_DIR}/src/parser/expression_tree.cpp
              ${CMAKE_SOURCE_DIR}/src/skip_data/SkipDataProcessor.cpp
              ${CMAKE_SOURCE_DIR}/src/skip_data/utils.cpp
              ${CMAKE_SOURCE_DIR}/src/cython/static.cpp
              ${CMAKE_SOURCE_DIR}/src/cython/initialize.cpp
              ${CMAKE_SOURCE_DIR}/src/cython/io.cpp
              ${CMAKE_SOURCE_DIR}/src/cython/errors.cpp
              ${CMAKE_SOURCE_DIR}/src/cython/engine.cpp
              ${CMAKE_SOURCE_DIR}/src/distribution/primitives.cpp

              ${CMAKE_SOURCE_DIR}/src/communication/factory/MessageFactory.cpp
              ${CMAKE_SOURCE_DIR}/src/communication/CommunicationData.cpp
              ${CMAKE_SOURCE_DIR}/src/communication/messages/MessageUtil.cu
              ${CMAKE_SOURCE_DIR}/src/communication/messages/GPUComponentMessage.cpp
              ${CMAKE_SOURCE_DIR}/src/communication/CommunicationInterface/serializer.cpp
              ${CMAKE_SOURCE_DIR}/src/communication/CommunicationInterface/node.cpp
              ${CMAKE_SOURCE_DIR}/src/communication/CommunicationInterface/messageReceiver.cpp
              ${CMAKE_SOURCE_DIR}/src/communication/CommunicationInterface/messageListener.cpp
              ${CMAKE_SOURCE_DIR}/src/communication/CommunicationInterface/bufferTransport.cpp
              ${CMAKE_SOURCE_DIR}/src/communication/CommunicationInterface/protocols.cpp
              ${CMAKE_SOURCE_DIR}/src/communication/CommunicationInterface/messageSender.cpp
        )

link_directories(${BSQL_BLD_PREFIX}/lib)

add_library(blazingsql-engine SHARED ${SRC_FILES})

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

    ${UCX_LIBRARIES}

    parquet
    arrow
    thrift
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

    gtest
    cudftestutil
)

###################################################################################################
# - build options ---------------------------------------------------------------------------------

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

# Print the project summary
feature_summary(WHAT ALL INCLUDE_QUIET_PACKAGES FATAL_ON_MISSING_REQUIRED_PACKAGES)

#END MAIN #
