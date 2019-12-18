#include "Common/HadoopFileSystemTest.h"

TEST(DisconnectTest, AfterConnection) {
	const std::unique_ptr<HadoopFileSystem> hadoopFileSystem =
		std::unique_ptr<HadoopFileSystem>(new HadoopFileSystem());
	const FileSystemConnection fileSystemConnection = SystemEnvironment::getLocalHadoopFileSystemConnection();
	const bool connected = hadoopFileSystem->connect(fileSystemConnection);

	EXPECT_TRUE(connected);

	const bool result = hadoopFileSystem->disconnect();

	EXPECT_TRUE(result);
}
