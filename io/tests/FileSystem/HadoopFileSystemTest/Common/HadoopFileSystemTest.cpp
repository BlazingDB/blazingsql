#include "HadoopFileSystemTest.h"

#include "FileSystem/HadoopFileSystem.h"

#include "SystemEnvironment.h"
#include "TestResources.h"

// BEGIN HadoopFileSystemBaseTest

void HadoopFileSystemBaseTest::init() {
	this->hadoopFileSystem = std::unique_ptr<HadoopFileSystem>(new HadoopFileSystem());

	const FileSystemConnection fileSystemConnection = SystemEnvironment::getLocalHadoopFileSystemConnection();

	// TODO check this
	this->hadoopFileSystem->connect(fileSystemConnection);

	TestResources::createTestResources();
}

void HadoopFileSystemBaseTest::shutdown() {
	// TODO percy check this
	// hadoopFileSystem->disconnect();

	TestResources::removeTestResources();
}

Uri HadoopFileSystemBaseTest::uri(const std::string path) const {
	return Uri(Uri::fileSystemTypeToScheme(FileSystemType::HDFS), "test_hdfs_disk1", Path(path));
}

// END HadoopFileSystemBaseTest

// BEGIN HadoopFileSystemTest

void HadoopFileSystemTest::SetUp() {
	HadoopFileSystemBaseTest::init();
	nativeHdfs.connectFromSystemEnvironment();
}

void HadoopFileSystemTest::TearDown() {
	HadoopFileSystemBaseTest::shutdown();
	nativeHdfs.disconnect();
}

// END HadoopFileSystemTest
