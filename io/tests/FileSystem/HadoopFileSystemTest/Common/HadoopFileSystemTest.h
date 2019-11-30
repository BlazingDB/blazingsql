#ifndef _TEST_FILESYSTEM_HADOOP_BASE_TEST_H_
#define _TEST_FILESYSTEM_HADOOP_BASE_TEST_H_

#include "gtest/gtest.h"

#include "FileSystem/HadoopFileSystem.h"

#include "NativeHdfs.h"
#include "SystemEnvironment.h"
#include "TestResources.h"

// TODO percy docs
// NOTE this class is the base for all hadoopfs tests
class HadoopFileSystemBaseTest {
protected:
	void init();	  // init hadoopFileSystem instance and test resources
	void shutdown();  // delete hadoopFileSystem instance and test resources

protected:
	Uri uri(const std::string path) const;

protected:
	std::unique_ptr<HadoopFileSystem> hadoopFileSystem;
};

// Classic test fixture
class HadoopFileSystemTest : public HadoopFileSystemBaseTest, public testing::Test {
protected:
	virtual void SetUp();
	virtual void TearDown();

	NativeHdfs nativeHdfs;
};

/**
 * Hadoop File System Tests
 * ------------------------
 *
 *                +------------+
 *                | HadoopTest |
 *                +------------+
 *                      |          With...
 *                      |
 *                      |     +------------+  +-----------+  +------+  +-------+
 *                      |     | Connection |  | Resources |  | HDFS |  | Clean |
 *                      |     +------------+  +-----------+  +------+  +-------+
 *                      |
 *    +-------------+   |
 *    | ConnectTest |---+
 *    +-------------+   |   +-----------+
 *                      +---| ExistTest |
 * +----------------+   |   +-----------+
 * | DisconnectTest |---|
 * +----------------+   |   +----------+
 *                      +---| ListTest |
 *                      |   +----------+
 *                      |
 *                      |   +-------------------+
 *                      +---| MakeDirectoryTest |
 *                      |   +-------------------+
 *                      |
 *                      |   +------------+
 *                      +---| RemoveTest |
 *                      |   +------------+
 *                      |
 *                      |   +-------------------+
 *                      +---| GetFileStatusTest |
 *                          +-------------------+
 */

#endif  // _TEST_FILESYSTEM_HADOOP_BASE_TEST_H_
