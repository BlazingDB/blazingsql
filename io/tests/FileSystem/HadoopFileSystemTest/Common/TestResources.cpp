#include "TestResources.h"

#include "NativeHdfs.h"

namespace TestResources {

void createTestResources() {
	NativeHdfs hdfs;

	hdfs.connectFromSystemEnvironment();

	hdfs.createFile("/empty_file");
	hdfs.createFile("/file01", "file01", 7);

	hdfs.makeDirectory("/resources");
	hdfs.makeDirectory("/resources/directory01");
	hdfs.createFile("/resources/empty_file");
	hdfs.createFile("/resources/file01", "file01", 7);

	// TODO check if fail
	hdfs.disconnect();
}

void removeTestResources() {
	NativeHdfs hdfs;

	hdfs.connectFromSystemEnvironment();

	hdfs.remove("/empty_file");
	hdfs.remove("/file01");

	hdfs.removeDirectory("/resources");

	// TODO check if fail
	hdfs.disconnect();
}

} // namespace TestResources
