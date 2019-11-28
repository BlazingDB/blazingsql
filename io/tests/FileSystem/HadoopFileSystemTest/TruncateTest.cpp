#include <string>

#include "Common/HadoopFileSystemTest.h"
#include "Common/NativeHdfs.h"
#include "Common/SystemEnvironment.h"

TEST_F(HadoopFileSystemTest, Truncate) {
	EXPECT_TRUE(hadoopFileSystem->truncateFile(uri("/resources/file01"), 4));

	NativeHdfs hdfs;
	hdfs.connectFromSystemEnvironment();

	std::shared_ptr<arrow::Buffer> result = hdfs.readFile("/resources/file01");

	arrow::Buffer expected(reinterpret_cast<const uint8_t *>("file"), 4);

	EXPECT_TRUE(result->Equals(expected));
}
