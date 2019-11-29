
set(CMAKE_CXX_FLAGS "-std=c++1z -O1 -Wall -Wextra -Werror")

set(GMOCK_BOTH_LIBRARIES gmock gmock_main)

set(BLAZING_SOURCE_DIR ${PROJECT_SOURCE_DIR}/src)

set(BLAZING_MOCK_DIR ${CMAKE_CURRENT_SOURCE_DIR}/Mock)


add_subdirectory(Unit/BlazingLogger)
add_subdirectory(Unit/CoutOutput)
add_subdirectory(Unit/Logger)
add_subdirectory(Unit/TcpOutput)
add_subdirectory(Integration)

## Optional Functionality
#add_subdirectory(Performance)
