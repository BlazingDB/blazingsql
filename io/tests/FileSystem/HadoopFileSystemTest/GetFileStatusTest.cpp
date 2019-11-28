#include "Common/HadoopFileSystemTest.h"

TEST_F(HadoopFileSystemTest, CheckFile01) {
	EXPECT_TRUE(hadoopFileSystem->getFileStatus(uri("/resources/file01")).isFile());
}

TEST_F(HadoopFileSystemTest, CheckDirectory01) {
	EXPECT_TRUE(hadoopFileSystem->getFileStatus(uri("/resources/directory01")).isDirectory());
}

TEST_F(HadoopFileSystemTest, NoExistentFileStatus) {
	FileStatus fileStatus = hadoopFileSystem->getFileStatus(uri("/no_existent_file"));
	EXPECT_FALSE(fileStatus.isFile());
	EXPECT_FALSE(fileStatus.isDirectory());
	EXPECT_EQ(FileType::UNDEFINED, fileStatus.getFileType());
	EXPECT_EQ(0, fileStatus.getFileSize());
}
