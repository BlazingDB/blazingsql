set(FileSystemRepositoryTest_SRCS
    FileSystemRepositoryTest.cpp
)

configure_test(FileSystemRepositoryTest "${FileSystemRepositoryTest_SRCS}" SimplicityFileSystem SimplicityUtil_StandardCppOnly)
