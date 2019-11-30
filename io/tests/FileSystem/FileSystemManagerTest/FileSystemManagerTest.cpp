#include <iostream>
#include <limits.h>
#include <time.h>

#include "gtest/gtest.h"

#include "FileSystem/FileSystemManager.h"

class FileSystemManagerTest : public testing::Test {
protected:
	FileSystemManagerTest() : fileSystemManager(new FileSystemManager()) {}

	virtual ~FileSystemManagerTest() {}

	virtual void SetUp() {}

	virtual void TearDown() {}

protected:
	const std::unique_ptr<FileSystemManager> fileSystemManager;
};

TEST_F(FileSystemManagerTest, CheckDefaultLocalFileSystem) {
	const std::string root = "/";

	const bool exists = fileSystemManager->exists(root);
	EXPECT_TRUE(exists);

	const FileStatus fileStatus = fileSystemManager->getFileStatus(root);
	EXPECT_TRUE(fileStatus.isDirectory());
}

TEST_F(FileSystemManagerTest, OtherTest) {
	//	const Path directory(rootDirectoryName);
	//
	//	const bool created = hadoopFileSystem->makeDirectory(directory);
	//	EXPECT_TRUE(created);
	//
	//	const bool exists = hadoopFileSystem->exists(directory);
	//	EXPECT_TRUE(exists);
	//
	//	const FileStatus fileStatus = hadoopFileSystem->getFileStatus(directory);
	//	EXPECT_FALSE(fileStatus.isFile());
	//	EXPECT_TRUE(fileStatus.isDirectory());
	//	EXPECT_FALSE(fileStatus.isSymlink());
	//	EXPECT_EQ(fileStatus.getPath(), directory);
}
