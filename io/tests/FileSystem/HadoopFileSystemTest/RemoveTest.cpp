#include "Common/HadoopFileSystemTest.h"

//TODO percy use the result pattern

TEST_F(HadoopFileSystemTest, RemoveDirectory) {
	EXPECT_TRUE(nativeHdfs.exists("/resources/directory01"));
	EXPECT_TRUE(hadoopFileSystem->remove(uri("/resources/directory01")));
	EXPECT_FALSE(nativeHdfs.exists("/resources/directory01"));
}

TEST_F(HadoopFileSystemTest, RemoveFile) {
	EXPECT_TRUE(nativeHdfs.exists("/resources/file01"));
	EXPECT_TRUE(hadoopFileSystem->remove(uri("/resources/file01")));
	EXPECT_FALSE(nativeHdfs.exists("/resources/file01"));
}

TEST_F(HadoopFileSystemTest, RemoveNoExistent) {
	EXPECT_FALSE(nativeHdfs.exists("/no_existent_file"));
	EXPECT_FALSE(hadoopFileSystem->remove(uri("/no_existent_file")));
}
