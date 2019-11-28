#=============================================================================
# Copyright 2018 BlazingDB, Inc.
#     Copyright 2018 Percy Camilo Triveño Aucahuasi <percy@blazingdb.com>
#=============================================================================

cmake_minimum_required(VERSION 2.8.12)

cmake_policy(SET CMP0048 NEW)

project(simplewebserver-download NONE)

include(ExternalProject)

ExternalProject_Add(simplewebserver
    GIT_REPOSITORY  ${SIMPLEWEBSERVER_GIT_REPOSITORY}
    UPDATE_COMMAND  ""
    CMAKE_ARGS      -DCMAKE_BUILD_TYPE=${SIMPLEWEBSERVER_BUILD_TYPE}
                    -DCMAKE_INSTALL_PREFIX:PATH=${CMAKE_BINARY_DIR}${CMAKE_FILES_DIRECTORY}/thirdparty/simplewebserver-install
                    ${SIMPLEWEBSERVER_CMAKE_ARGS}
)
