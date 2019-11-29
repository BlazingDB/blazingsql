#include <iostream>
#include <limits.h>
#include <time.h>

#include "gtest/gtest.h"

#include "arrow/status.h"

#include "FileSystem/S3FileSystem.h"

// TODO remove this (create a static method in S3Fiklesystem or ... somewhere)
#include <aws/core/Aws.h>

using namespace S3FileSystemConnection;

const std::string AUTHORITY = "jack_s3_1";  // the file system namespace used for this test

class S3FileSystemTest : public testing::Test {
protected:
	S3FileSystemTest() {
		Aws::SDKOptions sdkOptions;
		Aws::InitAPI(sdkOptions);

		const std::string bucketName = getBucketNameEnvValue();
		const EncryptionType encryptionType = getEncryptionTypeEnvValue();
		const std::string kmsKeyAmazonResourceName = getKmsKeyAmazonResourceNameEnvValue();
		const std::string accessKeyId = getAccessKeyIdEnvValue();
		const std::string secretKey = getSecretKeyEnvValue();
		const std::string sessionToken = getSessionTokenEnvValue();
		const FileSystemConnection fileSystemConnection(
			bucketName, encryptionType, kmsKeyAmazonResourceName, accessKeyId, secretKey, sessionToken);

		Path root;

		s3FileSystem.reset(new S3FileSystem(fileSystemConnection, root));
	}

	virtual ~S3FileSystemTest() {
		Aws::SDKOptions sdkOptions;
		Aws::ShutdownAPI(sdkOptions);
	}

	virtual void SetUp() {}

	virtual void TearDown() {
		// S3FileSystem->disconnect();
	}

private:
	// TODO percy duplicated code with HadoopFileSystemTest, create common test abstraccions for this kind of stuff
	std::string getConnectionPropertyEnvValue(ConnectionProperty connectionProperty) const {
		const std::string propertyEnvName = connectionPropertyEnvName(connectionProperty);
		const char * envValue = std::getenv(propertyEnvName.c_str());
		const bool isDefined = (envValue != nullptr);

		if(isDefined == false) {
			const std::string error = "FATAL: You need to define the environment variable: " + propertyEnvName;
			throw std::invalid_argument(error);
		}

		const std::string propertyEnvValue = isDefined ? std::string(envValue) : std::string();

		return propertyEnvValue;
	}

	const std::string getBucketNameEnvValue() const {
		const std::string value = getConnectionPropertyEnvValue(ConnectionProperty::BUCKET_NAME);
		return value;
	}

	const EncryptionType getEncryptionTypeEnvValue() const {
		const std::string value = getConnectionPropertyEnvValue(ConnectionProperty::ENCRYPTION_TYPE);
		const EncryptionType encryptionType = encryptionTypeFromName(value);
		return encryptionType;
	}

	const std::string getKmsKeyAmazonResourceNameEnvValue() const {
		const std::string value = getConnectionPropertyEnvValue(ConnectionProperty::KMS_KEY_AMAZON_RESOURCE_NAME);
		return value;
	}

	const std::string getAccessKeyIdEnvValue() const {
		const std::string value = getConnectionPropertyEnvValue(ConnectionProperty::ACCESS_KEY_ID);
		return value;
	}

	const std::string getSecretKeyEnvValue() const {
		const std::string value = getConnectionPropertyEnvValue(ConnectionProperty::SECRET_KEY);
		return value;
	}

	const std::string getSessionTokenEnvValue() const {
		const std::string value = getConnectionPropertyEnvValue(ConnectionProperty::SESSION_TOKEN);
		return value;
	}

protected:
	std::unique_ptr<S3FileSystem> s3FileSystem;
};


const std::string rootDirectoryName = "/test";

TEST_F(S3FileSystemTest, CreateDirectoryAndDeleteDirectoryWSlash) {
	const Uri dir(FileSystemType::S3, AUTHORITY, Path("/new folder 2/"));

	const bool created = s3FileSystem->makeDirectory(dir);
	EXPECT_TRUE(created);

	bool exists = s3FileSystem->exists(dir);
	EXPECT_TRUE(exists);

	const FileStatus fileStatus = s3FileSystem->getFileStatus(dir);
	EXPECT_FALSE(fileStatus.isFile());
	EXPECT_TRUE(fileStatus.isDirectory());

	const bool deleted = s3FileSystem->remove(dir);
	EXPECT_TRUE(deleted);

	exists = s3FileSystem->exists(dir);
	EXPECT_FALSE(exists);
}

TEST_F(S3FileSystemTest, CreateDirectoryAndDeleteDirectoryWoSlash) {
	const Uri dir(FileSystemType::S3, AUTHORITY, Path("/new folder 3"));

	const bool created = s3FileSystem->makeDirectory(dir);
	EXPECT_TRUE(created);

	bool exists = s3FileSystem->exists(dir);
	EXPECT_TRUE(exists);

	const FileStatus fileStatus = s3FileSystem->getFileStatus(dir);
	EXPECT_FALSE(fileStatus.isFile());
	EXPECT_TRUE(fileStatus.isDirectory());

	const bool deleted = s3FileSystem->remove(dir);
	EXPECT_TRUE(deleted);

	exists = s3FileSystem->exists(dir);
	EXPECT_FALSE(exists);
}

TEST_F(S3FileSystemTest, DeleteDirectoryThatDoesNotExistWoSlash) {
	const Uri dir(FileSystemType::S3, AUTHORITY, Path("/new folder 4"));

	bool exists = s3FileSystem->exists(dir);
	EXPECT_FALSE(exists);

	const bool deleted = s3FileSystem->remove(dir);
	EXPECT_TRUE(deleted);
}

TEST_F(S3FileSystemTest, DeleteDirectoryThatDoesNotExistWSlash) {
	const Uri dir(FileSystemType::S3, AUTHORITY, Path("/new folder 5/"));

	bool exists = s3FileSystem->exists(dir);
	EXPECT_FALSE(exists);

	const bool deleted = s3FileSystem->remove(dir);
	EXPECT_TRUE(deleted);
}

TEST_F(S3FileSystemTest, DeleteFileThatDoesNotExist) {
	const Uri dir(FileSystemType::S3, AUTHORITY, Path("/newFile.txt"));

	bool exists = s3FileSystem->exists(dir);
	EXPECT_FALSE(exists);

	bool deleted = s3FileSystem->remove(dir);
	EXPECT_TRUE(deleted);

	const Uri dir2(FileSystemType::S3, AUTHORITY, Path("/newfolder6/newFile.txt"));

	exists = s3FileSystem->exists(dir2);
	EXPECT_FALSE(exists);

	deleted = s3FileSystem->remove(dir2);
	EXPECT_TRUE(deleted);
}

TEST_F(S3FileSystemTest, MoveDirectory) {
	const Uri ddir1(FileSystemType::S3, AUTHORITY, Path("/dirForMoveTest1/"));
	const Uri ddir2(FileSystemType::S3, AUTHORITY, Path("/dirForMoveTest2/"));

	// TEST : move ddir2 into ddir1

	// setup
	if(!s3FileSystem->exists(ddir1)) {
		const bool dirCreated = s3FileSystem->makeDirectory(ddir1);
		EXPECT_TRUE(dirCreated);
	}
	if(!s3FileSystem->exists(ddir2)) {
		const bool dirCreated = s3FileSystem->makeDirectory(ddir2);
		EXPECT_TRUE(dirCreated);
	}

	// action
	const Uri destination = ddir1 + "/dir2/";
	const bool moved = s3FileSystem->move(ddir2, destination);
	EXPECT_TRUE(moved);

	// verification
	const bool origExists = s3FileSystem->exists(ddir2);
	EXPECT_FALSE(origExists);
	const bool newExists = s3FileSystem->exists(destination);
	EXPECT_TRUE(newExists);


	// tear down
	if(s3FileSystem->exists(ddir1)) {
		const bool removed = s3FileSystem->remove(ddir1);
		EXPECT_TRUE(removed);
	}
	if(s3FileSystem->exists(ddir2)) {
		const bool removed = s3FileSystem->remove(ddir2);
		EXPECT_TRUE(removed);
	}
	if(s3FileSystem->exists(destination)) {
		const bool removed = s3FileSystem->remove(destination);
		EXPECT_TRUE(removed);
	}
}

TEST_F(S3FileSystemTest, MoveFile) {
	const Uri src(FileSystemType::S3, AUTHORITY, Path("/fileForMoveTestSource.txt"));
	const Uri dest(FileSystemType::S3, AUTHORITY, Path("/fileForMoveTestDest.txt"));

	// TEST : move src to dest

	std::string fileContents = "testing move file";

	// setup
	if(!s3FileSystem->exists(src)) {
		std::shared_ptr<arrow::io::OutputStream> outputStream = s3FileSystem->openWriteable(src);
		outputStream->Write(fileContents);
		outputStream->Close();

		const bool exists = s3FileSystem->exists(src);
		EXPECT_TRUE(exists);
	}
	if(s3FileSystem->exists(dest)) {
		const bool removed = s3FileSystem->remove(dest);
		EXPECT_TRUE(removed);
	}

	// action
	const bool moved = s3FileSystem->move(src, dest);
	EXPECT_TRUE(moved);


	// verification
	const bool origExists = s3FileSystem->exists(src);
	EXPECT_FALSE(origExists);
	const bool newExists = s3FileSystem->exists(dest);
	EXPECT_TRUE(newExists);

	std::shared_ptr<arrow::io::RandomAccessFile> file = s3FileSystem->openReadable(dest);
	int64_t fileSize;
	file->GetSize(&fileSize);

	std::string verificationContents;
	verificationContents.resize(fileSize);

	int64_t totalRead;
	arrow::Status status = file->Read(fileSize, &totalRead, (uint8_t *) &verificationContents[0]);
	EXPECT_TRUE(status.ok());
	file->Close();

	EXPECT_TRUE(verificationContents == fileContents);

	// tear down
	if(s3FileSystem->exists(src)) {
		const bool removed = s3FileSystem->remove(src);
		EXPECT_TRUE(removed);
	}
	if(s3FileSystem->exists(dest)) {
		const bool removed = s3FileSystem->remove(dest);
		EXPECT_TRUE(removed);
	}
}

TEST_F(S3FileSystemTest, CanListBucketRoot) {
	const std::set<std::string> dirs = {"/root", "/home", "/etc"};
	const std::vector<Uri> files = s3FileSystem->list(Uri(FileSystemType::S3, AUTHORITY, Path("/tpch-50mb/")));

	int foundCount = 0;

	EXPECT_TRUE(files.size() == 6);

	for(const Uri & uri : files) {
		// TODO create a STD common code (for test only?) where we can put helpers like find(value, container)
		// const bool found = (dirs.find(path.toString()) != dirs.end());

		std::cout << uri.toString(false) << std::endl;

		// if (found) {
		// TODO improve this part of the test
		// EXPECT_TRUE(path.isDirectory());
		//++foundCount;
		//}
	}

	// EXPECT_EQ(foundCount, dirs.size());
}

TEST_F(S3FileSystemTest, CanListBucketRootWoSlash) {
	const std::set<std::string> dirs = {"/root", "/home", "/etc"};
	const std::vector<Uri> files = s3FileSystem->list(Uri(FileSystemType::S3, AUTHORITY, Path("/tpch-50mb")));

	int foundCount = 0;

	EXPECT_TRUE(files.size() == 6);

	for(const Uri & uri : files) {
		// TODO create a STD common code (for test only?) where we can put helpers like find(value, container)
		// const bool found = (dirs.find(path.toString()) != dirs.end());

		std::cout << uri.toString(false) << std::endl;

		// if (found) {
		// TODO improve this part of the test
		// EXPECT_TRUE(path.isDirectory());
		//++foundCount;
		//}
	}

	// EXPECT_EQ(foundCount, dirs.size());
}

TEST_F(S3FileSystemTest, FileStatusForFile) {
	const FileStatus status =
		s3FileSystem->getFileStatus(Uri(FileSystemType::S3, AUTHORITY, Path("/tpch-50mb/nation.tbl")));

	EXPECT_TRUE(status.getFileType() == FileType::FILE);
	EXPECT_TRUE(status.getFileSize() == 2224);
}

TEST_F(S3FileSystemTest, FileStatusForFolderWoSlash) {
	const FileStatus status = s3FileSystem->getFileStatus(Uri(FileSystemType::S3, AUTHORITY, Path("/tpch-50mb")));

	EXPECT_TRUE(status.getFileType() == FileType::DIRECTORY);
}

TEST_F(S3FileSystemTest, FileStatusForFolderWSlash) {
	const FileStatus status = s3FileSystem->getFileStatus(Uri(FileSystemType::S3, AUTHORITY, Path("/tpch-50mb/")));

	EXPECT_TRUE(status.getFileType() == FileType::DIRECTORY);
}

TEST_F(S3FileSystemTest, FileExistForFileThatIsThere) {
	bool exists = s3FileSystem->exists(Uri(FileSystemType::S3, AUTHORITY, Path("/tpch-50mb/nation.tbl")));

	EXPECT_TRUE(exists);
}
TEST_F(S3FileSystemTest, FileExistForFileThatIsNotThere) {
	bool exists = s3FileSystem->exists(Uri(FileSystemType::S3, AUTHORITY, Path("/tpch-50mb/nation.tbll")));

	EXPECT_FALSE(exists);
}
TEST_F(S3FileSystemTest, FileExistForDirWithSlashThere) {
	bool exists = s3FileSystem->exists(Uri(FileSystemType::S3, AUTHORITY, Path("/tpch-50mb/")));

	EXPECT_TRUE(exists);
}
TEST_F(S3FileSystemTest, FileExistForDirWoSlashThere) {
	bool exists = s3FileSystem->exists(Uri(FileSystemType::S3, AUTHORITY, Path("/tpch-50mb")));

	EXPECT_TRUE(exists);
}
TEST_F(S3FileSystemTest, FileExistForDirWithSlashNotThere) {
	bool exists = s3FileSystem->exists(Uri(FileSystemType::S3, AUTHORITY, Path("/tpch-50mbasd/")));

	EXPECT_FALSE(exists);
}
TEST_F(S3FileSystemTest, FileExistForDirWoSlashNotThere) {
	bool exists = s3FileSystem->exists(Uri(FileSystemType::S3, AUTHORITY, Path("/tpch-50mbasdf")));

	EXPECT_FALSE(exists);
}

TEST_F(S3FileSystemTest, ExistDirectoryEncryptedBucket) {
	const Uri dir(FileSystemType::S3, AUTHORITY, Path("/tpch-50mb/"));
	const bool exists = s3FileSystem->exists(dir);

	EXPECT_TRUE(exists);
}
