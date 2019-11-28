set(GoogleCloudStorageTest_SRCS
    GoogleCloudStorageTest.cpp
    ${PROJECT_SOURCE_DIR}/src/ExceptionHandling/BlazingException.cpp
)

configure_test(GoogleCloudStorageTest "${GoogleCloudStorageTest_SRCS}" SimplicityFileSystem SimplicityUtil_StandardCppOnly)
