#include "Common/HadoopFileSystemTest.h"

TEST_F(HadoopFileSystemTest, CheckNotExistentFile) {
	const bool result = hadoopFileSystem->exists(uri("/no_existent_file"));

	EXPECT_FALSE(result);
}

TEST_F(HadoopFileSystemTest, CheckResources) {
	const bool result = hadoopFileSystem->exists(uri("/resources"));

	EXPECT_TRUE(result);
}
