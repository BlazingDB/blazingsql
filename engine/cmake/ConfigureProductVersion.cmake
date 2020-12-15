#=============================================================================
# Copyright 2018 BlazingDB, Inc.
#     Copyright 2018-2020 Percy Camilo Triveño Aucahuasi <percy@blazingsql.com>
#=============================================================================

function(join list_elements separator output)
    string(REGEX REPLACE "([^\\]|^);" "\\1${separator}" _TMP_STR "${list_elements}")
    string(REGEX REPLACE "[\\](.)" "\\1" _TMP_STR "${_TMP_STR}") #fixes escaping
    set(${output} "${_TMP_STR}" PARENT_SCOPE)
endfunction()


function(configure_bsqlengine_config_header)
    set(LSB_RELEASE_FILE /etc/lsb-release)
    set(OS_RELEASE_FILE /etc/os-release)

    if(EXISTS ${LSB_RELEASE_FILE})
        file(STRINGS ${LSB_RELEASE_FILE} LIST_ELEMENTS)
        join("${LIST_ELEMENTS}" "|" LSB_RELEASE)
        string(REPLACE "\"" "" LSB_RELEASE ${LSB_RELEASE})
    endif()

    if(EXISTS ${OS_RELEASE_FILE})
        file(STRINGS ${OS_RELEASE_FILE} LIST_ELEMENTS)
        join("${LIST_ELEMENTS}" "|" OS_RELEASE)
        string(REPLACE "\"" "" OS_RELEASE ${OS_RELEASE})
    endif()

    configure_file(${PROJECT_SOURCE_DIR}/bsqlengine_config.h.cmake ${PROJECT_BINARY_DIR}/bsqlengine_config.h)
endfunction()


# This target will update the git (and other info from CLIs) information in the header git-config-bsql-engine.h, see config-bsql-engine.h.cmake
add_custom_target(UpdateBSQLEngineInternalConfig
    COMMAND bash ${PROJECT_SOURCE_DIR}/update_bsqlengine_config.sh "${PROJECT_BINARY_DIR}"
    WORKING_DIRECTORY ${PROJECT_SOURCE_DIR}/..)

#execute_process(
#    COMMAND bash ${PROJECT_SOURCE_DIR}/scripts/build-google-cloud-cpp.sh "${PROJECT_SOURCE_DIR}"
#    RESULT_VARIABLE result
#    WORKING_DIRECTORY ${PROJECT_SOURCE_DIR}
#)

configure_bsqlengine_config_header()

#add_custom_command(
#    TARGET blazingsql-engine
#    PRE_BUILD
#    COMMAND 
#    WORKING_DIRECTORY ${PROJECT_SOURCE_DIR}/../
#)

# NOTE percy when build ral cpp
# Each time we build the target we update the config-bsql-engine.h file and thus always have the last git commit hash (without run cmake again)
add_dependencies(blazingsql-engine UpdateBSQLEngineInternalConfig)
