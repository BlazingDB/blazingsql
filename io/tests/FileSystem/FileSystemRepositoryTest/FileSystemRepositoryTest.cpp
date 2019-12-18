#include <memory>

#include "gtest/gtest.h"

#include "FileSystem/FileSystemRepository.h"

class FileSystemRepositoryTest {
public:
	FileSystemRepositoryTest(bool encrypted) {
		const std::string dataFile = "/tmp/filesystemrepositorytest";
		this->fileSystemRepository =
			std::unique_ptr<FileSystemRepository>(new FileSystemRepository(std::move(dataFile), encrypted));
	}

protected:
	std::unique_ptr<FileSystemRepository> fileSystemRepository;
};

class UnencryptedFileSystemRepositoryTest : public FileSystemRepositoryTest {
public:
	UnencryptedFileSystemRepositoryTest() : FileSystemRepositoryTest(false) {}
};

class EncryptedFileSystemRepositoryTest : public FileSystemRepositoryTest {
public:
	EncryptedFileSystemRepositoryTest() : FileSystemRepositoryTest(true) {}
};

class AddFileSystemTest : public testing::TestWithParam<FileSystemEntity> {
protected:
	virtual void SetUp() {
		const FileSystemEntity param = GetParam();  // test parameter

		this->fileSystemEntity = param;
	}

	virtual void TearDown() {}

protected:
	FileSystemEntity fileSystemEntity;
};

class AddUnencryptedFileSystemTest : public AddFileSystemTest, public UnencryptedFileSystemRepositoryTest {};
class AddEncryptedFileSystemTest : public AddFileSystemTest, public EncryptedFileSystemRepositoryTest {};

class AddValidUnencryptedFileSystemTest : public AddUnencryptedFileSystemTest {};
class AddValidEncryptedFileSystemTest : public AddEncryptedFileSystemTest {};

class AddInvalidUnencryptedFileSystemTest : public AddUnencryptedFileSystemTest {};
class AddInvalidEncryptedFileSystemTest : public AddEncryptedFileSystemTest {};

class FindAllFileSystemTest : public testing::Test {
protected:
	virtual void SetUp() {}
	virtual void TearDown() {}
};

class FindAllUnencryptedFileSystemTest : public FindAllFileSystemTest, public UnencryptedFileSystemRepositoryTest {};
class FindAllEncryptedFileSystemTest : public FindAllFileSystemTest, public EncryptedFileSystemRepositoryTest {};

class DeleteFileSystemTest : public testing::Test {
protected:
	virtual void SetUp() {}
	virtual void TearDown() {}
};

class DeleteUnencryptedFileSystemTest : public DeleteFileSystemTest, public UnencryptedFileSystemRepositoryTest {};
class DeleteEncryptedFileSystemTest : public DeleteFileSystemTest, public EncryptedFileSystemRepositoryTest {};

const std::vector<FileSystemEntity> validFileSystems = {
	// BEGIN LOCAL

	FileSystemEntity("local_disk1",  // authority
		FileSystemConnection(		 // valid FileSystemConnection instance
			FileSystemType::LOCAL),
		Path("/hi/")  // root
		),

	// END LOCAL

	// BEGIN HDFS

	FileSystemEntity("hdf_disk1",  // authority
		FileSystemConnection(	  // valid FileSystemConnection instance
			"127.0.0.1",
			457,
			"percy",
			HadoopFileSystemConnection::DriverType::LIBHDFS3,
			"kerberosTicket33"),
		Path("/hola/a")  // root
		),

	// END HDFS

	// BEGIN S3

	FileSystemEntity("percy_s3_b1",  // authority
		FileSystemConnection(		 // valid FileSystemConnection instance
			"simply-test",
			S3FileSystemConnection::EncryptionType::NONE,
			"",
			"accessKeyId2",
			"secretKey33",
			"sessionToken11"),
		Path("/")  // root
		),

	FileSystemEntity("percy_s3_aes256",  // authority
		FileSystemConnection(			 // valid FileSystemConnection instance
			"simply-test",
			S3FileSystemConnection::EncryptionType::AES_256,
			"",
			"accessKeyId2",
			"secretKey33",
			"sessionToken11"),
		Path("/hi/")  // root
		),

	FileSystemEntity("percy_s3_awskms",  // authority
		FileSystemConnection(			 // valid FileSystemConnection instance
			"simply-test",
			S3FileSystemConnection::EncryptionType::AWS_KMS,
			"arn:aws:a3sdmasterkey",
			"accessKeyId2",
			"secretKey33",
			""),
		Path("/hi/hello/")  // root
		)

	// END S3
};

const std::vector<FileSystemEntity> invalidFileSystems = {
	FileSystemEntity("local_disk1",  // authority
		FileSystemConnection(		 // invalid FileSystemConnection instance
			FileSystemType::S3),
		Path("/hi/")  // root
		),

	// BEGIN HDFS

	FileSystemEntity("hdf_disk1",  // authority
		FileSystemConnection(	  // invalid FileSystemConnection instance
			"127.0.0.1",
			-457,
			"percy",
			HadoopFileSystemConnection::DriverType::LIBHDFS3,
			"kerberosTicket33"),
		Path("/hola/a")  // root
		),

	FileSystemEntity("hdf_disk1",  // authority
		FileSystemConnection(	  // invalid FileSystemConnection instance
			"127.0.0.1",
			457,
			"percy",
			HadoopFileSystemConnection::DriverType::UNDEFINED,
			"kerberosTicket33"),
		Path("/hola/a")  // root
		),

	FileSystemEntity("hdf_disk1",  // authority
		FileSystemConnection(	  // invalid FileSystemConnection instance
			"127.0.0.1",
			457,
			"percy",
			HadoopFileSystemConnection::DriverType::LIBHDFS,
			"kerberosTicket33"),
		Path("invalid asd")  // root
		),

	// END HDFS

	// BEGIN S3

	FileSystemEntity("percy_s3_b1",  // authority
		FileSystemConnection(		 // invalid FileSystemConnection instance
			"simply-test",
			S3FileSystemConnection::EncryptionType::UNDEFINED,
			"",
			"accessKeyId2",
			"secretKey33",
			"sessionToken11"),
		Path("/")  // root
		),

	FileSystemEntity("percy_s3_aes256",  // authority
		FileSystemConnection(			 // invalid FileSystemConnection instance
			"simply-test",
			S3FileSystemConnection::EncryptionType::AWS_KMS,
			"",  // if EncryptionType::AWS_KMS this field should not be empty
			"accessKeyId2",
			"secretKey33",
			"sessionToken11"),
		Path("/hi/")  // root
		),

	FileSystemEntity("percy_s3_awskms",  // authority
		FileSystemConnection(			 // invalid FileSystemConnection instance
			"simply-test",
			S3FileSystemConnection::EncryptionType::AWS_KMS,
			"arn:aws:a3sdmasterkey",
			"",
			"",
			""),
		Path("/hi/hello/")  // root
		)

	// END S3
};

static void checkAddValidFileSystem(
	const std::unique_ptr<FileSystemRepository> & fileSystemRepository, const FileSystemEntity & fileSystemEntity) {
	const std::string dataFile = fileSystemRepository->getDataFile().toString(true);

	std::remove(dataFile.c_str());

	EXPECT_TRUE(fileSystemEntity.isValid());

	const bool ok = fileSystemRepository->add(fileSystemEntity);

	EXPECT_TRUE(ok);

	const auto all = fileSystemRepository->findAll();

	EXPECT_EQ(all.size(), 1);
	EXPECT_EQ(all[0], fileSystemEntity);

	std::remove(dataFile.c_str());
}

static void checkAddInvalidFileSystem(
	const std::unique_ptr<FileSystemRepository> & fileSystemRepository, const FileSystemEntity & fileSystemEntity) {
	const std::string dataFile = fileSystemRepository->getDataFile().toString(true);

	std::remove(dataFile.c_str());

	EXPECT_FALSE(fileSystemEntity.isValid());

	const bool ok = fileSystemRepository->add(fileSystemEntity);

	EXPECT_FALSE(ok);

	const auto all = fileSystemRepository->findAll();

	EXPECT_TRUE(all.empty());

	std::remove(dataFile.c_str());
}

static void checkFindAllFileSystem(const std::unique_ptr<FileSystemRepository> & fileSystemRepository) {
	const std::string dataFile = fileSystemRepository->getDataFile().toString(true);

	std::remove(dataFile.c_str());

	for(const FileSystemEntity & fileSystemEntity : validFileSystems) {
		fileSystemRepository->add(fileSystemEntity);
	}

	const auto all = fileSystemRepository->findAll();

	EXPECT_EQ(all.size(), validFileSystems.size());

	for(int i = 0; i < all.size(); ++i) {
		EXPECT_EQ(all[i], validFileSystems[i]);
	}

	std::remove(dataFile.c_str());
}

static void checkDeleteFileSystem(const std::unique_ptr<FileSystemRepository> & fileSystemRepository) {
	const std::string dataFile = fileSystemRepository->getDataFile().toString(true);

	std::remove(dataFile.c_str());

	for(const FileSystemEntity & fileSystemEntity : validFileSystems) {
		fileSystemRepository->add(fileSystemEntity);
	}

	const int deletedIndex = 0;  // we will delete the first element
	const std::string & authority = validFileSystems.at(deletedIndex).getAuthority();
	const bool ok = fileSystemRepository->deleteByAuthority(authority);

	EXPECT_TRUE(ok);

	const auto all = fileSystemRepository->findAll();

	EXPECT_EQ(all.size(), validFileSystems.size() - 1);

	int loadedIndex = 0;
	for(int i = 0; i < validFileSystems.size(); ++i) {
		if(i == loadedIndex) {
			continue;
		}

		EXPECT_EQ(all[loadedIndex], validFileSystems[i]);

		++loadedIndex;
	}

	std::remove(dataFile.c_str());
}

TEST_P(AddValidUnencryptedFileSystemTest, CheckAddValidUnencryptedFileSystem) {
	checkAddValidFileSystem(fileSystemRepository, fileSystemEntity);
}

TEST_P(AddValidEncryptedFileSystemTest, CheckAddValidEncryptedFileSystem) {
	checkAddValidFileSystem(fileSystemRepository, fileSystemEntity);
}

TEST_P(AddInvalidUnencryptedFileSystemTest, CheckAddInvalidUnencryptedFileSystem) {
	checkAddInvalidFileSystem(fileSystemRepository, fileSystemEntity);
}

TEST_P(AddInvalidEncryptedFileSystemTest, CheckAddInvalidEncryptedFileSystem) {
	checkAddInvalidFileSystem(fileSystemRepository, fileSystemEntity);
}

TEST_F(FindAllUnencryptedFileSystemTest, CheckFindAllUnencryptedFileSystem) {
	checkFindAllFileSystem(fileSystemRepository);
}

TEST_F(FindAllEncryptedFileSystemTest, CheckFindAllEncryptedFileSystem) {
	checkFindAllFileSystem(fileSystemRepository);
}

TEST_F(DeleteUnencryptedFileSystemTest, CheckDeleteUnencryptedFileSystem) {
	checkDeleteFileSystem(fileSystemRepository);
}

TEST_F(DeleteEncryptedFileSystemTest, CheckDeleteEncryptedFileSystem) { checkDeleteFileSystem(fileSystemRepository); }

INSTANTIATE_TEST_CASE_P(
	AddValidUnencryptedFileSystemTestCase, AddValidUnencryptedFileSystemTest, testing::ValuesIn(validFileSystems));
INSTANTIATE_TEST_CASE_P(
	AddValidEncryptedFileSystemTestCase, AddValidEncryptedFileSystemTest, testing::ValuesIn(validFileSystems));

INSTANTIATE_TEST_CASE_P(AddInvalidUnencryptedFileSystemTestCase,
	AddInvalidUnencryptedFileSystemTest,
	testing::ValuesIn(invalidFileSystems));
INSTANTIATE_TEST_CASE_P(
	AddInvalidEncryptedFileSystemTestCase, AddInvalidEncryptedFileSystemTest, testing::ValuesIn(invalidFileSystems));
