#=============================================================================
# Copyright 2018 BlazingDB, Inc.
#     Copyright 2018 Percy Camilo Triveño Aucahuasi <percy@blazingdb.com>
#     Copyright 2018 Christian Noboa <christian@blazingdb.com>
#=============================================================================

# BEGIN macros

# TODO percy fix CONFIGURE_GPU_COMPUTE_CAPABILITY impl
# check the cmake arg -DGPU_COMPUTE_CAPABILITY and if not present defines its default value
macro(CONFIGURE_GPU_COMPUTE_CAPABILITY)
    #GPU_COMPUTE_CAPABILITY 30 means GPU compute capability version 3.0

    if(NOT GPU_COMPUTE_CAPABILITY)
        message(AUTHOR_WARNING "NVIDIA GPU Compute Capability is not defined, using NVIDIA GPU compute capability version 6.1")
        set(GPU_COMPUTE_CAPABILITY "61")
    endif ()
endmacro()

macro(CONFIGURE_CUDA_LIBRARIES)
    set(CUDA_LIBRARY_DIR       ${CUDA_TOOLKIT_ROOT_DIR})
    set(CUDA_LIBRARY_STUBS_DIR ${CUDA_TOOLKIT_ROOT_DIR})

    ## Based on NVRTC (Runtime Compilation) - CUDA Toolkit Documentation - v9.2.148
    ## 2.2. Installation
    if(CMAKE_HOST_APPLE)
        set(CUDA_LIBRARY_DIR       ${CUDA_TOOLKIT_ROOT_DIR}/lib)
        set(CUDA_LIBRARY_STUBS_DIR ${CUDA_TOOLKIT_ROOT_DIR}/lib/stubs)
    elseif(CMAKE_HOST_UNIX)
        set(CUDA_LIBRARY_DIR       ${CUDA_TOOLKIT_ROOT_DIR}/lib64)
        set(CUDA_LIBRARY_STUBS_DIR ${CUDA_TOOLKIT_ROOT_DIR}/lib64/stubs)
    elseif(CMAKE_HOST_WIN32)
        set(CUDA_LIBRARY_DIR       ${CUDA_TOOLKIT_ROOT_DIR}\lib\x64)
        set(CUDA_LIBRARY_STUBS_DIR ${CUDA_TOOLKIT_ROOT_DIR}\lib\x64\stubs)
    endif()

    set(CUDA_CUDA_LIBRARY  cuda)
    set(CUDA_NVRTC_LIBRARY nvrtc)
    set(CUDA_NVTX_LIBRARY nvToolsExt)

    message(STATUS "CUDA_CUDA_LIBRARY: ${CUDA_CUDA_LIBRARY}")
    message(STATUS "CUDA_NVRTC_LIBRARY: ${CUDA_NVRTC_LIBRARY}")
    message(STATUS "CUDA_NVTX_LIBRARY: ${CUDA_NVTX_LIBRARY}")
    message(STATUS "CUDA_LIBRARY_DIR: ${CUDA_LIBRARY_DIR}")
    message(STATUS "CUDA_LIBRARY_STUBS_DIR: ${CUDA_LIBRARY_STUBS_DIR}")

    # TODO percy seems cmake bug: we cannot define target dirs per cuda target
    # ... see if works in future cmake versions
    link_directories(${CUDA_LIBRARY_DIR} ${CUDA_LIBRARY_STUBS_DIR} ${CMAKE_CUDA_IMPLICIT_LINK_DIRECTORIES})
endmacro()

# compute_capability is a int value (e.g. 30 means compute capability 3.0)
macro(CONFIGURE_CUDA_COMPILER compute_capability)
    include_directories(${CUDA_INCLUDE_DIRS})

    set(CMAKE_CUDA_FLAGS "${CMAKE_CUDA_FLAGS} -gencode=arch=compute_60,code=sm_60 -gencode=arch=compute_61,code=sm_61")
    #set(CMAKE_CUDA_FLAGS "${CMAKE_CUDA_FLAGS} -gencode=arch=compute_60,code=sm_60")
    set(CMAKE_CUDA_FLAGS "${CMAKE_CUDA_FLAGS} -gencode=arch=compute_70,code=sm_70 -gencode=arch=compute_70,code=compute_70")

    set(CMAKE_CUDA_FLAGS "${CMAKE_CUDA_FLAGS} --expt-extended-lambda --expt-relaxed-constexpr")

    # suppress SHFL warnings caused by Modern GPU
    # TODO: remove this when Modern GPU is removed or fixed to use shfl_sync
    set(CMAKE_CUDA_FLAGS "${CMAKE_CUDA_FLAGS} -Werror cross-execution-space-call -Wno-deprecated-declarations -Xptxas --disable-warnings")

    # set warnings
    set(CMAKE_CUDA_FLAGS "${CMAKE_CUDA_FLAGS} -Werror cross-execution-space-call -Xcompiler -Wall")

    if(CMAKE_BUILD_TYPE MATCHES Debug)
        message(STATUS "Building with debugging flags")
        set(CMAKE_CUDA_FLAGS "${CMAKE_CUDA_FLAGS} -G -Xcompiler -rdynamic")
    set(CUDA_NVCC_FLAGS "${CUDA_NVCC_FLAGS} -G -g" )
        set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -rdynamic")
    endif(CMAKE_BUILD_TYPE MATCHES Debug)

    message(STATUS "Default C++ (CUDA) compiler flags for all targets: ${CMAKE_CUDA_FLAGS}")
    message(STATUS "Default C++ compiler flags for all targets: ${CMAKE_CXX_FLAGS}")
    message(STATUS "Default CUDA compiler flags for all targets: ${CUDA_NVCC_FLAGS}")
endmacro()

# END macros


# BEGIN MAIN #

# set(CUDA_SDK_ROOT_DIR "/usr/local/cuda") # /usr/local/cuda is the standard installation directory
# find_package(CUDA REQUIRED)
# set_package_properties(CUDA PROPERTIES TYPE REQUIRED
#     PURPOSE "NVIDIA CUDA® parallel computing platform and programming model."
#     URL "https://developer.nvidia.com/cuda-zone"
# )

#if(NOT CUDA_FOUND)
#    message(FATAL_ERROR "CUDA not found, please check your settings.")
#endif()

message(STATUS "CUDA ${CUDA_VERSION} found in ${CUDA_TOOLKIT_ROOT_DIR}")

configure_gpu_compute_capability()
configure_cuda_libraries()
configure_cuda_compiler(${GPU_COMPUTE_CAPABILITY})

# END MAIN #
