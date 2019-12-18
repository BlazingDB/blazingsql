#=============================================================================
# Copyright 2019 BlazingDB, Inc.
#     Copyright 2019 Percy Camilo Triveño Aucahuasi <percy@blazingdb.com>
#=============================================================================

if (BLAZINGDB_DEPENDENCIES_INSTALL_DIR)
    message(STATUS "BLAZINGDB_DEPENDENCIES_INSTALL_DIR defined, it will use ${BLAZINGDB_DEPENDENCIES_INSTALL_DIR}")
else()
    message(FATAL_ERROR "BLAZINGDB_DEPENDENCIES_INSTALL_DIR not defined, please call cmake with -DBLAZINGDB_DEPENDENCIES_INSTALL_DIR")
endif()

set(CMAKE_MODULE_PATH "${BLAZINGDB_DEPENDENCIES_INSTALL_DIR}/lib/cmake/" ${CMAKE_MODULE_PATH})
message(STATUS "CMAKE_MODULE_PATH (with blazingdb-dependencies cmake modules): ${CMAKE_MODULE_PATH}")
