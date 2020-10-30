#=============================================================================
# Copyright 2018 BlazingDB, Inc.
#     Copyright 2018 Percy Camilo Triveño Aucahuasi <percy@blazingdb.com>
#=============================================================================

# BEGIN macros

macro(CONFIGURE_SIMPLEWEBSERVER_EXTERNAL_PROJECT)


    # NOTE percy c.gonzales if you want to pass other RAL CMAKE_CXX_FLAGS into this dependency add it by harcoding
    set(SIMPLEWEBSERVER_CMAKE_ARGS
    )

    # Download and unpack simplewebserver at configure time
    configure_file(${CMAKE_CURRENT_LIST_DIR}/SimpleWebServer.CMakeLists.txt.cmake ${CMAKE_BINARY_DIR}${CMAKE_FILES_DIRECTORY}/thirdparty/simplewebserver-download/CMakeLists.txt)

    execute_process(
        COMMAND ${CMAKE_COMMAND} -G "${CMAKE_GENERATOR}" .
        RESULT_VARIABLE result
        WORKING_DIRECTORY ${CMAKE_BINARY_DIR}${CMAKE_FILES_DIRECTORY}/thirdparty/simplewebserver-download/
    )

    if(result)
        message(FATAL_ERROR "CMake step for simplewebserver failed: ${result}")
    endif()

    execute_process(
        COMMAND ${CMAKE_COMMAND} --build . -- -j8
        RESULT_VARIABLE result
        WORKING_DIRECTORY ${CMAKE_BINARY_DIR}${CMAKE_FILES_DIRECTORY}/thirdparty/simplewebserver-download/
    )

    if(result)
        message(FATAL_ERROR "Build step for simplewebserver failed: ${result}")
    endif()
endmacro()

# END macros

# BEGIN MAIN #

if (SIMPLEWEBSERVER_INSTALL_DIR)
    message(STATUS "SIMPLEWEBSERVER_INSTALL_DIR defined, it will use vendor version from ${SIMPLEWEBSERVER_INSTALL_DIR}")
    set(SIMPLEWEBSERVER_ROOT "${SIMPLEWEBSERVER_INSTALL_DIR}")
else()
    message(STATUS "SIMPLEWEBSERVER_INSTALL_DIR not defined, it will be built from sources")
    configure_simplewebserver_external_project()
    set(SIMPLEWEBSERVER_ROOT "${CMAKE_BINARY_DIR}${CMAKE_FILES_DIRECTORY}/thirdparty/simplewebserver-install/")
endif()

find_package(SimpleWebServer REQUIRED)
set_package_properties(SimpleWebServer PROPERTIES TYPE REQUIRED
    PURPOSE "SimpleWebServer."
    URL "www.")

if(NOT SIMPLEWEBSERVER_FOUND)
    message(FATAL_ERROR "simplewebserver not found, please check your settings.")
endif()

message(STATUS "simplewebserver found in ${SIMPLEWEBSERVER_ROOT}")

include_directories(${SIMPLEWEBSERVER_INCLUDEDIR} ${SIMPLEWEBSERVER_INCLUDE_DIR})

link_directories(${SIMPLEWEBSERVER_LIBDIR})

# END MAIN #
