#=============================================================================
# Copyright 2019 BlazingDB, Inc.
#     Copyright 2019 Percy Camilo Triveño Aucahuasi <percy@blazingdb.com>
#=============================================================================

# - Find BlazingDB protocol C++ library libblazingdb-io (libblazingdb-io.a)
# SIMPLEWEBSERVER_ROOT hints the location
#
# This module defines
# SIMPLEWEBSERVER_FOUND
# SIMPLEWEBSERVER_INCLUDEDIR Preferred include directory e.g. <prefix>/include
# SIMPLEWEBSERVER_INCLUDE_DIR, directory containing blazingdb-io headers
# SIMPLEWEBSERVER_LIBS, blazingdb-io libraries
# SIMPLEWEBSERVER_LIBDIR, directory containing blazingdb-io libraries
# SIMPLEWEBSERVER_STATIC_LIB, path to blazingdb-io.a
# blazingdb-io - static library

# If SIMPLEWEBSERVER_ROOT is not defined try to search in the default system path
if ("${SIMPLEWEBSERVER_ROOT}" STREQUAL "")
    set(SIMPLEWEBSERVER_ROOT "/usr")
endif()

set(SIMPLEWEBSERVER_SEARCH_LIB_PATH
  ${SIMPLEWEBSERVER_ROOT}/lib
  ${SIMPLEWEBSERVER_ROOT}/lib/x86_64-linux-gnu
  ${SIMPLEWEBSERVER_ROOT}/lib64
  ${SIMPLEWEBSERVER_ROOT}/build
)

set(SIMPLEWEBSERVER_SEARCH_INCLUDE_DIR
  ${SIMPLEWEBSERVER_ROOT}/include/simple-web-server/
)

find_path(SIMPLEWEBSERVER_INCLUDE_DIR server_http.hpp
    PATHS ${SIMPLEWEBSERVER_SEARCH_INCLUDE_DIR}
    NO_DEFAULT_PATH
    DOC "Path to blazingdb-io headers"
)

if (NOT SIMPLEWEBSERVER_INCLUDE_DIR)
    message(FATAL_ERROR "blazingdb-io includes and libraries NOT found. "
      "Looked for headers in ${SIMPLEWEBSERVER_SEARCH_INCLUDE_DIR}")
    set(SIMPLEWEBSERVER_FOUND FALSE)
else()
    set(SIMPLEWEBSERVER_INCLUDE_DIR ${SIMPLEWEBSERVER_ROOT}/include/)
    set(SIMPLEWEBSERVER_INCLUDEDIR ${SIMPLEWEBSERVER_ROOT}/include/)
    set(SIMPLEWEBSERVER_LIBDIR ${SIMPLEWEBSERVER_ROOT}/build) # TODO percy make this part cross platform
    set(SIMPLEWEBSERVER_FOUND TRUE)
    #add_library(blazingdb-io STATIC IMPORTED)
    #set_target_properties(blazingdb-io PROPERTIES IMPORTED_LOCATION "${SIMPLEWEBSERVER_STATIC_LIB}")
endif ()

mark_as_advanced(
  SIMPLEWEBSERVER_FOUND
  SIMPLEWEBSERVER_INCLUDEDIR
  SIMPLEWEBSERVER_INCLUDE_DIR
  #SIMPLEWEBSERVER_LIBS
  SIMPLEWEBSERVER_STATIC_LIB
  #blazingdb-io
)
