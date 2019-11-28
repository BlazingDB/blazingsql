#include "Common/HadoopFileSystemTest.h"

//TODO percy use the result pattern

TEST_F(HadoopFileSystemTest, File01) {
	EXPECT_TRUE(nativeHdfs.exists("/resources/file01"));
	EXPECT_FALSE(nativeHdfs.exists("/resources/moved"));
	EXPECT_TRUE(hadoopFileSystem->move(uri("/resources/file01"), uri("/resources/moved")));
	EXPECT_FALSE(nativeHdfs.exists("/resources/file01"));
	EXPECT_TRUE(nativeHdfs.exists("/resources/moved"));
}

TEST_F(HadoopFileSystemTest, NoExistentFile) {
	EXPECT_FALSE(nativeHdfs.exists("/no_existent_file"));
	EXPECT_FALSE(hadoopFileSystem->move(uri("/no_existent_file"), uri("/moved")));
}
