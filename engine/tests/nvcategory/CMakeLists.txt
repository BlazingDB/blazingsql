include_directories(
    ${CMAKE_SOURCE_DIR}/thirdparty/jitify)

set(source_files
    ${CMAKE_SOURCE_DIR}/src/Config/Config.cpp

)

set(nvcategory_test_SRCS
    ${CMAKE_SOURCE_DIR}/src/Utils.cu
    ${CMAKE_SOURCE_DIR}/src/GDFCounter.cu
    ${CMAKE_SOURCE_DIR}/src/GDFColumn.cu
    ${CMAKE_SOURCE_DIR}/src/ColumnManipulation.cu
    ${CMAKE_SOURCE_DIR}/src/CodeTimer.cpp
    ${CMAKE_SOURCE_DIR}/src/LogicalFilter.cpp
    ${CMAKE_SOURCE_DIR}/src/CalciteExpressionParsing.cpp
    ${CMAKE_SOURCE_DIR}/src/Interpreter/interpreter_cpp.cu
    ${CMAKE_SOURCE_DIR}/src/CalciteInterpreter.cpp
    ${source_files}
    ${CMAKE_SOURCE_DIR}/src/cuDF/Allocator.cpp
    nvcategory-test.cu
)

configure_test(nvcategory-test "${nvcategory_test_SRCS}")


target_link_libraries(nvcategory-test
                      ${cuDF_adapter_testing_library}
                      ${cuDF_adapter_testing_dependencies}
)
