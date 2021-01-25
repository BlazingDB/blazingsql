set(StringUtilTest_SRCS
    StringUtilTest.cpp
)

configure_test(StringUtilTest "${StringUtilTest_SRCS}" SimplicityFileSystem SimplicityUtil_StandardCppOnly)
