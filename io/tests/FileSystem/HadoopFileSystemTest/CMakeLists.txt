set(HadoopFileSystemTest_Common_SRCS
    Common/SystemEnvironment.cpp
    Common/NativeHdfs.cpp
    Common/TestResources.cpp
    Common/HadoopFileSystemTest.cpp
)

set(HadoopFileSystemTest_SRCS
    ${HadoopFileSystemTest_Common_SRCS}
    ConnectTest.cpp
    DisconnectTest.cpp
    ExistsTest.cpp
    GetFileStatusTest.cpp
    ListTest.cpp
    MakeDirectoryTest.cpp
    MoveTest.cpp
    RemoveTest.cpp
    OpenReadableTest.cpp
    OpenWriteableTest.cpp
    TruncateTest.cpp
)

configure_test(HadoopFileSystemTest "${HadoopFileSystemTest_SRCS}" SimplicityFileSystem SimplicityUtil_StandardCppOnly)
