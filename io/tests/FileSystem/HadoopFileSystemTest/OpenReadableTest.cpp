#include "Common/HadoopFileSystemTest.h"

#include "arrow/buffer.h"
#include "arrow/status.h"

TEST_F(HadoopFileSystemTest, NoExistent) {
	std::shared_ptr<arrow::io::RandomAccessFile> file = hadoopFileSystem->openReadable(uri("/no_existent"));

	EXPECT_EQ(nullptr, file);
}

TEST_F(HadoopFileSystemTest, EmptyFile) {
	std::shared_ptr<arrow::io::RandomAccessFile> file = hadoopFileSystem->openReadable(uri("/resources/empty_file"));
	ASSERT_NE(nullptr, file);

	int64_t size;

	ASSERT_TRUE(file->GetSize(&size).ok());

	std::shared_ptr<arrow::Buffer> out;
	uint64_t nbytes = 0;

	EXPECT_EQ(nbytes, size);
	EXPECT_TRUE(file->Read(nbytes, &out).ok());
	EXPECT_EQ(nbytes, out->size());

	EXPECT_STREQ(nullptr, reinterpret_cast<const char *>(out->data()));
}

TEST_F(HadoopFileSystemTest, File) {
	std::shared_ptr<arrow::io::RandomAccessFile> file = hadoopFileSystem->openReadable(uri("/resources/file01"));
	ASSERT_NE(nullptr, file);

	int64_t size;

	ASSERT_TRUE(file->GetSize(&size).ok());

	std::shared_ptr<arrow::Buffer> out;
	uint64_t nbytes = 7;

	EXPECT_EQ(nbytes, size);
	EXPECT_TRUE(file->Read(nbytes, &out).ok());
	EXPECT_EQ(nbytes, out->size());

	EXPECT_STREQ("file01", reinterpret_cast<const char *>(out->data()));
}
