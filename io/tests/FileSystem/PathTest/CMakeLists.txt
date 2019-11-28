set(PathTest_SRCS
    PathTest.cpp
)

configure_test(PathTest "${PathTest_SRCS}" SimplicityFileSystem SimplicityUtil_StandardCppOnly)
