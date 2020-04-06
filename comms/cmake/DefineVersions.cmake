#=============================================================================
# Copyright 2019 BlazingDB, Inc.
#     Copyright 2019 Percy Camilo Triveño Aucahuasi <percy@blazingdb.com>
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#=============================================================================

# BEGIN macros

function(PRINT_LINE)
    message(STATUS "************************************************************************************************")
endfunction()

macro(DEFINE_DEFAULT_BUILD_TYPE
    component_name
    default_build_type)

    if(DEFINED ${component_name}_BUILD_TYPE)
        message(STATUS "${component_name}_BUILD_TYPE is defined!")
    else()
        set(${component_name}_BUILD_TYPE ${default_build_type})
        message(STATUS "Using the default build type for ${component_name}_BUILD_TYPE: ${${component_name}_BUILD_TYPE}")
    endif()
endmacro()

macro(DEFINE_DEFAULT_GIT_PACKAGE
    component_name
    default_git_repository
    default_git_tag
    default_build_type)

    message(STATUS "${component_name} information:")

    if(DEFINED ${component_name}_GIT_REPOSITORY)
        message(STATUS "${component_name}_GIT_REPOSITORY is defined!")
    else()
        set(${component_name}_GIT_REPOSITORY ${default_git_repository})
        message(STATUS "Using the default repository for ${component_name}_GIT_REPOSITORY: ${${component_name}_GIT_REPOSITORY}")
    endif()

    if(DEFINED ${component_name}_GIT_TAG)
        message(STATUS "${component_name}_GIT_TAG is defined!")
    else()
        set(${component_name}_GIT_TAG ${default_git_tag})
        message(STATUS "Using the default tag for ${component_name}_GIT_TAG: ${${component_name}_GIT_TAG}")
    endif()

    define_default_build_type(${component_name} ${default_build_type})

    message(STATUS "${component_name}_GIT_REPOSITORY: ${${component_name}_GIT_REPOSITORY}")
    message(STATUS "${component_name}_GIT_TAG: ${${component_name}_GIT_TAG}")
    message(STATUS "${component_name}_BUILD_TYPE: ${${component_name}_BUILD_TYPE}")

    # TODO put here some listener to catch and save the versions for the config.h
    # at the and make some kind of print summary

    print_line()
endmacro()

macro(DEFINE_DEFAULT_GENERIC_PACKAGE
    component_name
    default_url
    default_version
    default_build_type)

    message(STATUS "${component_name} information:")

    if(DEFINED ${component_name}_URL)
        message(STATUS "${component_name}_URL is defined!")
    else()
        set(${component_name}_URL ${default_url})
        message(STATUS "Using the default URL for ${component_name}_URL: ${${component_name}_URL}")
    endif()

    define_default_build_type(${component_name} ${default_build_type})

    message(STATUS "${component_name}_URL: ${${component_name}_URL}")
    message(STATUS "${component_name}_VERSION: ${default_version}")
    message(STATUS "${component_name}_BUILD_TYPE: ${${component_name}_BUILD_TYPE}")

    # TODO put here some listener to catch and save the versions for the config.h
    # at the and make some kind of print summary

    print_line()
endmacro()

macro(DEFINE_DEFAULT_PYTHON_PACKAGE
    default_version)

    if(DEFINED PYTHON3_VERSION)
        message(STATUS "PYTHON3_VERSION is defined!")
    else()
        set(PYTHON3_VERSION ${default_version})
        message(STATUS "Using the default PYTHON3_VERSION: ${PYTHON3_VERSION}")
    endif()

    set(PYTHON3_URL "")

    if(PYTHON3_VERSION STREQUAL "3.6")
        set(PYTHON3_URL "https://anaconda.org/conda-forge/python/3.6.7/download/linux-64/python-3.6.7-h381d211_1004.tar.bz2")
    elseif(PYTHON3_VERSION STREQUAL "3.7")
        set(PYTHON3_URL "https://anaconda.org/conda-forge/python/3.7.3/download/linux-64/python-3.7.3-h5b0a415_0.tar.bz2")
    else()
        message(FATAL_ERROR "Invalid Python3 version, please use: 3.6 or 3.7")
    endif()

    define_default_generic_package(
        "PYTHON3"
        ${PYTHON3_URL}
        ${PYTHON3_VERSION}
        "Release"
    )
endmacro()

# END macros


# BEGIN MAIN #

print_line()


# NOTE SIMPLEWEBSERVER: the commit 3f8fcc0c311e8d7d2a13aa34253778bc8021ac14 is the stable release v2.1.1
define_default_git_package(
    "SIMPLEWEBSERVER"
    "https://github.com/BlazingDB/simplewebserver.git"
    "master"
    "Release"
)

define_default_git_package(
    "AWS_SDK_CPP"
    "https://github.com/aws/aws-sdk-cpp.git"
    "c60299701915c781ebb57c3213b8bdad854a885f"
    "Release"
)

define_default_python_package(
    "3.6"
)


# END MAIN #
