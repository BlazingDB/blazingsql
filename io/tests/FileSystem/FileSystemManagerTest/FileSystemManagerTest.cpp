#include <iostream>
#include <limits.h>
#include <time.h>

#include "gtest/gtest.h"

#include "FileSystem/FileSystemManager.h"
#include "Config/BlazingContext.h"

class FileSystemManagerTest : public testing::Test {
protected:
	FileSystemManagerTest() : fileSystemManager(new FileSystemManager()) {}

	virtual ~FileSystemManagerTest() {}

	virtual void SetUp() {
		// Init AWS S3 ... TODO see if we need to call shutdown and avoid leaks from s3 percy
		BlazingContext::getInstance()->initExternalSystems();
	}

	virtual void TearDown() {
		BlazingContext::getInstance()->shutDownExternalSystems();
	}

protected:
	const std::unique_ptr<FileSystemManager> fileSystemManager;
};

std::pair<bool, std::string> registerFileSystem(FileSystemConnection fileSystemConnection, std::string root, std::string authority) {
	Path rootPath(root);
	if(rootPath.isValid() == false) {
		std::string msg =
			"Invalid root " + root + " for filesystem " + authority + " :" + fileSystemConnection.toString();
		return std::make_pair(false, msg);
	}
	FileSystemEntity fileSystemEntity(authority, fileSystemConnection, rootPath);
	try {
		bool ok = BlazingContext::getInstance()->getFileSystemManager()->deregisterFileSystem(authority);
		ok = BlazingContext::getInstance()->getFileSystemManager()->registerFileSystem(fileSystemEntity);
		if (ok == false) {
			return std::make_pair(false, "Filesystem failed to register");
		}
	} catch(const std::exception & e) {
		return std::make_pair(false, std::string(e.what()));
	} catch(...) {
		std::string msg =
			std::string("Unknown error for filesystem " + authority + " :" + fileSystemConnection.toString());
		return std::make_pair(false, msg);
	}
	return std::make_pair(true, "");
}

TEST_F(FileSystemManagerTest, CheckDefaultLocalFileSystem) {
    FileSystemConnection fs1(FileSystemType::LOCAL);
    auto result = registerFileSystem(fs1, "/", "fs1");
    EXPECT_TRUE(result.first);
    
    result = registerFileSystem(fs1, "/", "fs1");
    EXPECT_TRUE(result.first);
}
