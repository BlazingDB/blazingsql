#include "Common/HadoopFileSystemTest.h"

TEST_F(HadoopFileSystemTest, Creation) {
	const Uri dir = uri("/new_directory");

	hadoopFileSystem->remove(dir);

	const bool exists = hadoopFileSystem->exists(dir);

	EXPECT_FALSE(exists);

	const bool created = hadoopFileSystem->makeDirectory(dir);

	EXPECT_TRUE(created);

	nativeHdfs.removeDirectory("/new_directory");
}
