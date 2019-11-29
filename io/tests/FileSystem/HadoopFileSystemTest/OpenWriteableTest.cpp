#include "Common/HadoopFileSystemTest.h"

#include "arrow/status.h"

TEST_F(HadoopFileSystemTest, Write) {
	std::shared_ptr<arrow::io::OutputStream> out = hadoopFileSystem->openWriteable(uri("/writeable"));

	out->Write("writeable");
	out->Flush();
	out->Close();

	//TODO use test subject api here
	EXPECT_EQ("writeable", nativeHdfs.readTextFile("/writeable"));

  nativeHdfs.remove("/writeable");
}
