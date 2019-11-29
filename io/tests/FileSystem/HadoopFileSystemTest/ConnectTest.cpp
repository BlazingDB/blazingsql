#include "Common/HadoopFileSystemTest.h"

TEST(ConnectTest, ValidConnection) {
	const std::unique_ptr<HadoopFileSystem> hadoopFileSystem =
		std::unique_ptr<HadoopFileSystem>(new HadoopFileSystem());
	const FileSystemConnection fileSystemConnection = SystemEnvironment::getLocalHadoopFileSystemConnection();
	const bool result = hadoopFileSystem->connect(fileSystemConnection);

	EXPECT_TRUE(result);
}

TEST(ConnectTest, UnknownHost) {
	const std::string host = "255.255.255.255";
	int port = 54310;
	const std::string user = "sdfasfdfsauser1";
	const HadoopFileSystemConnection::DriverType driverType = HadoopFileSystemConnection::DriverType::LIBHDFS3;
	const std::string kerberosTicket = "";
	const FileSystemConnection fileSystemConnection(host, port, user, driverType, kerberosTicket);
	const std::unique_ptr<HadoopFileSystem> hadoopFileSystem =
		std::unique_ptr<HadoopFileSystem>(new HadoopFileSystem());
	const bool result = hadoopFileSystem->connect(fileSystemConnection);

	EXPECT_FALSE(result);
}
