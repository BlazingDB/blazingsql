set(S3FileSystemTest_SRCS
    S3FileSystemTest.cpp
    ParquetOnS3Test.cpp
    ${PROJECT_SOURCE_DIR}/src/ExceptionHandling/BlazingException.cpp
)

configure_test(S3FileSystemTest "${S3FileSystemTest_SRCS}" SimplicityFileSystem SimplicityUtil_StandardCppOnly)
