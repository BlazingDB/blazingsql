#=============================================================================
# Copyright 2018 BlazingDB, Inc.
#     Copyright 2018 Percy Camilo Triveño Aucahuasi <percy@blazingdb.com>
#=============================================================================

cmake_minimum_required(VERSION 3.11)

project(aws-sdk-cpp-download NONE)

include(ExternalProject)

ExternalProject_Add(aws-sdk-cpp
    GIT_REPOSITORY  ${AWS_SDK_CPP_GIT_REPOSITORY}
    GIT_TAG         ${AWS_SDK_CPP_GIT_TAG}
    SOURCE_DIR      "${CMAKE_BINARY_DIR}${CMAKE_FILES_DIRECTORY}/thirdparty/aws-sdk-cpp-src"
    BINARY_DIR      "${CMAKE_BINARY_DIR}${CMAKE_FILES_DIRECTORY}/thirdparty/aws-sdk-cpp-build"
    INSTALL_DIR     "${CMAKE_BINARY_DIR}${CMAKE_FILES_DIRECTORY}/thirdparty/aws-sdk-cpp-install"
    UPDATE_COMMAND  ""
    GIT_SHALLOW     1
    CMAKE_ARGS      -DCMAKE_BUILD_TYPE=${AWS_SDK_CPP_BUILD_TYPE}
                    -DBUILD_ONLY=${AWS_MODULES_STR}
                    -DCMAKE_INSTALL_PREFIX:PATH=${CMAKE_BINARY_DIR}${CMAKE_FILES_DIRECTORY}/thirdparty/aws-sdk-cpp-install
                    ${AWS_SDK_CPP_CMAKE_ARGS}
)
