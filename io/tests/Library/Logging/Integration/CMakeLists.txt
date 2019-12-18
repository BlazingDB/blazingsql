
## Test name
set(BLAZING_TEST_NAME LibraryLoggingIntegrationTest)


## Set sources
set(BLAZING_SOURCE_FILES "${CMAKE_CURRENT_SOURCE_DIR}/LibraryLoggingIntegrationTest.cpp"
                         "${BLAZING_SOURCE_DIR}/Library/Logging/BlazingLogger.cpp"
                         "${BLAZING_SOURCE_DIR}/Library/Logging/CoutOutput.cpp"
                         "${BLAZING_SOURCE_DIR}/Library/Logging/Logger.cpp"
                         "${BLAZING_SOURCE_DIR}/Library/Logging/LoggingLevel.cpp"
                         "${BLAZING_SOURCE_DIR}/Library/Logging/ServiceLogging.cpp"
                         "${BLAZING_SOURCE_DIR}/Library/Logging/TcpOutput.cpp")


## Add test
add_executable(${BLAZING_TEST_NAME} ${BLAZING_SOURCE_FILES})
add_test(${BLAZING_TEST_NAME} ${BLAZING_TEST_NAME})


## Clear include header property
set_target_properties(${BLAZING_TEST_NAME} PROPERTIES INCLUDE_DIRECTORIES "")


## Include headers
target_include_directories(${BLAZING_TEST_NAME} PUBLIC "${BLAZING_MOCK_DIR}/Library/Network")
target_include_directories(${BLAZING_TEST_NAME} PUBLIC "${BLAZING_SOURCE_DIR}")


## Link libraries
target_link_libraries(${BLAZING_TEST_NAME} ${GTEST_BOTH_LIBRARIES})
target_link_libraries(${BLAZING_TEST_NAME} ${GMOCK_BOTH_LIBRARIES})
target_link_libraries(${BLAZING_TEST_NAME} ${CMAKE_THREAD_LIBS_INIT})
