#include <iostream>
#include <limits.h>
#include <time.h>

#include "gtest/gtest.h"

#include "FileSystem/LocalFileSystem.h"

class LocalFileSystemTest : public testing::Test {
	protected:
		LocalFileSystemTest() : localFileSystem(new LocalFileSystem()) {
		}

		virtual ~LocalFileSystemTest() {

		}

		virtual void SetUp() {
			//TODO just a dummy call for the case of local fs
			//localFileSystem->connect();
		}

		virtual void TearDown() {
			//TODO just a dummy call for the case of local fs
			//localFileSystem->disconnect();
		}

	protected:
		const std::unique_ptr<LocalFileSystem> localFileSystem;
};

TEST_F(LocalFileSystemTest, GetFileStatusLinuxRegularFile) {
	const std::string currentExe = "/proc/self/exe";
	const FileStatus fileStatus = localFileSystem->getFileStatus(currentExe);

	EXPECT_TRUE(fileStatus.isFile());
	EXPECT_FALSE(fileStatus.isDirectory());
	EXPECT_EQ(fileStatus.getUri().getPath().toString(true), currentExe);
	EXPECT_TRUE(fileStatus.getFileSize() > 0);
}

TEST_F(LocalFileSystemTest, GetFileStatusLinuxDirectory) {
	const std::string currentExe = "/proc/self/net";
	const FileStatus fileStatus = localFileSystem->getFileStatus(currentExe);

	EXPECT_FALSE(fileStatus.isFile());
	EXPECT_TRUE(fileStatus.isDirectory());
	EXPECT_EQ(fileStatus.getUri().getPath().toString(true), currentExe);
	EXPECT_EQ(fileStatus.getFileSize(), 0);
}

TEST_F(LocalFileSystemTest, CanListLinuxRootDirectories) {
	const std::set<std::string> dirs = {"/root", "/home", "/etc"};

	const std::vector<Uri> files = localFileSystem->list(Uri("/"));

	int foundCount = 0;

	for (const Uri &uri : files) {
		const Path path = uri.getPath();

		//TODO create a STD common code (for test only?) where we can put helpers like find(value, container)
		const bool found = (dirs.find(path.toString()) != dirs.end());

		if (found) {
			//TODO improve this part of the test
			//EXPECT_TRUE(path.isDirectory());
			++foundCount;
		}
	}

	EXPECT_EQ(foundCount, dirs.size());
}

TEST_F(LocalFileSystemTest, CheckIgnoreDotAnd2DotsWhenListLinuxRoot) {
	const std::vector<Uri> files = localFileSystem->list(Uri("/"));

	for (const Uri &uri : files) {
		const Path path = uri.getPath();
		const std::string test = path.toString(true);
		const bool found1DotOr2Dots =  (test == "/.") || (test == "/..");
		EXPECT_FALSE(found1DotOr2Dots);
	}
}
