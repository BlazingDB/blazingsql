#include <iostream>
#include <limits.h>
#include <time.h>

#include "gtest/gtest.h"

#include "FileSystem/FileSystemCommandParser.h"

struct ValidRegisterFileSystemCommand {
	ValidRegisterFileSystemCommand(const std::string &command, const FileSystemEntity &expectedResult)
		: command(command),
		  expectedResult(expectedResult) {
	}

	std::string command; // input: valid register file system command
	FileSystemEntity expectedResult; // expected result: valid FileSystemEntity instance
};

class ParseValidRegisterFileSystemCommandTest : public testing::TestWithParam<ValidRegisterFileSystemCommand> {
	protected:
		virtual void SetUp() {
			const ValidRegisterFileSystemCommand param = GetParam(); // test parameter

			this->command = param.command;
			this->expectedResult = param.expectedResult;
		}

		virtual void TearDown() {}

	protected:
		std::string command;
		FileSystemEntity expectedResult;
};

class ParseInvalidRegisterFileSystemCommandTest : public testing::TestWithParam<std::string> {
	protected:
		virtual void SetUp() {
			const std::string param = GetParam(); // test parameter

			this->command = param;
		}

		virtual void TearDown() {}

	protected:
		std::string command;
};

const std::vector<ValidRegisterFileSystemCommand> validRegisterFileSystemCommands = {
	//BEGIN LOCAL
	ValidRegisterFileSystemCommand(
		"register local file system stored as 'local_disk1' root '/usr/share/bz/opt/'", // input: valid register file system command
		FileSystemEntity(  // expected result: valid FileSystemEntity instance
			"local_disk1",
			FileSystemConnection(FileSystemType::LOCAL),
			Path("/usr/share/bz/opt/", true)
		)
	),
	ValidRegisterFileSystemCommand(
		"register local file system stored as 'local_disk2'", // input: valid register file system command
		FileSystemEntity(  // expected result: valid FileSystemEntity instance
			"local_disk2",
			FileSystemConnection(FileSystemType::LOCAL)
		)
	),
	//END LOCAL

	//BEGIN HDFS
	ValidRegisterFileSystemCommand(
		"register hdfs file system stored as 'percy_hdfs1' with ('127.0.0.1', 54310, 'percy', 'LIBHDFS3', 'KERBEROS_TICKET_sdfS') root '/user1/warehouse/spark/data/temp/'", // input: valid register file system command
		FileSystemEntity(  // expected result: valid FileSystemEntity instance
			"percy_hdfs1",
			FileSystemConnection(
				"127.0.0.1",
				54310,
				"percy",
				HadoopFileSystemConnection::DriverType::LIBHDFS3,
				"KERBEROS_TICKET_sdfS"
			),
			Path("/user1/warehouse/spark/data/temp/", true)
		)
	),
	ValidRegisterFileSystemCommand(
		"register hdfs file system stored as 'percy_hdfs22' with ('127.0.0.11', 54311, 'percyt', 'LIBHDFS3', 'KERBEROS_TICKET_sdfS45')", // input: valid register file system command
		FileSystemEntity(  // expected result: valid FileSystemEntity instance
			"percy_hdfs22",
			FileSystemConnection(
				"127.0.0.11",
				54311,
				"percyt",
				HadoopFileSystemConnection::DriverType::LIBHDFS3,
				"KERBEROS_TICKET_sdfS45"
			)
		)
	),
	//END HDFS

	//BEGIN S3
	ValidRegisterFileSystemCommand(
		"register s3 file system stored as 'bz_s3_testing' with ('bucket1', 'None', '', 'accessKeyIddsf3', 'secretKey234', 'sessionToken3s3dfds') root '/root/tpch/tables/'", // input: valid register file system command
		FileSystemEntity(  // expected result: valid FileSystemEntity instance
			"bz_s3_testing",
			FileSystemConnection(
				"bucket1",
				S3FileSystemConnection::EncryptionType::NONE,
				"",
				"accessKeyIddsf3",
				"secretKey234",
				"sessionToken3s3dfds"
			),
			Path("/root/tpch/tables/", true)
		)
	),
	ValidRegisterFileSystemCommand(
		"register s3 file system stored as 'simplicity_testing_encrypted' with ('stest_1', 'AES-256', '', 'accessKey444', 'secretKeyAA', 'sessionTokenDDD') root '/files/dts/'", // input: valid register file system command
		FileSystemEntity(  // expected result: valid FileSystemEntity instance
			"simplicity_testing_encrypted",
			FileSystemConnection(
				"stest_1",
				S3FileSystemConnection::EncryptionType::AES_256,
				"",
				"accessKey444",
				"secretKeyAA",
				"sessionTokenDDD"
			),
			Path("/files/dts/", true)
		)
	),
	ValidRegisterFileSystemCommand(
		"register s3 file system stored as 'simplicity_testing_encrypted_kms' with ('buk123', 'AWS-KMS', 'arn:aws:dasadasw', 'accessKeyO9', 'secretKey1A', 'sessionToken9') root '/__new_/files'", // input: valid register file system command
		FileSystemEntity(  // expected result: valid FileSystemEntity instance
			"simplicity_testing_encrypted_kms",
			FileSystemConnection(
				"buk123",
				S3FileSystemConnection::EncryptionType::AWS_KMS,
				"arn:aws:dasadasw",
				"accessKeyO9",
				"secretKey1A",
				"sessionToken9"
			),
			Path("/__new_/files", true)
		)
	),
	ValidRegisterFileSystemCommand(
		"register s3 file system stored as 'nm-s3-kms-no-root' with ('buk123', 'AWS-KMS', 'arn:aws:dasadasw', 'accessKeyO9', 'secretKey1A', 'sessionToken9')", // input: valid register file system command
		FileSystemEntity(  // expected result: valid FileSystemEntity instance
			"nm-s3-kms-no-root",
			FileSystemConnection(
				"buk123",
				S3FileSystemConnection::EncryptionType::AWS_KMS,
				"arn:aws:dasadasw",
				"accessKeyO9",
				"secretKey1A",
				"sessionToken9"
			)
		)
	)
	//END S3
};

const std::vector<std::string> invalidRegisterFileSystemCommands = {
	"",
	"r",
	"randomstring",
	"random phrase",
	"more random phrase",
	"+ more random phrase with  more stuff /",
	"register local file system stored as 'local_disk1' with (EXTRA_ARG) root '/usr/share/bz/opt/'"
	"register hdfs file system stored as 'percy_hdfs1' with ('EXTRA_ARG', '127.0.0.1', 54310, 'percy', 'LIBHDFS3', 'KERBEROS_TICKET_sdfS') root '/user1/warehouse/spark/data/temp/'",
	"register hdfs file system stored as 'badd_root' with ('127.0.0.1', 54310, 'percy', 'LIBHDFS3', 'KERBEROS_TICKET_sdfS') root '1 /asd asda / asdad/ a_0'",
	"register s3 file system stored as 'nm-s3-kms-no-root' with ('EXTRA_ARG', 'buk123', 'AWS-KMS', 'arn:aws:dasadasw', 'accessKeyO9', 'secretKey1A', 'sessionToken9')"
	"register s3 file system stored as 'kms-EMPTY_ARN' with ('encrypted_bucket_kms1', 'AWS-KMS', '', 'accessKeyO9', 'secretKey1A', 'sessionToken9')"
	"register s3 file system stored as 'kms-fail1' with ('encrypted_bucket_kms1', , , 'AWS-KMS', 'adasdd', 'accessKeyO9', 'secretKey1A', 'sessionToken9')"
	"register s3 file system stored as 'kms-fail2' root '/user1/warehouse/spark/data/temp/'"
	"register s3 file system stored as 'kms-fail3'"
	"register s3 file system stored as 'kms-fail4"
};

TEST_P(ParseValidRegisterFileSystemCommandTest, CheckParseValidRegisterFileSystemCommand) {
	std::string error;
	const FileSystemEntity fileSystemEntity = FileSystemCommandParser::parseRegisterFileSystem(command, error);
	const FileSystemConnection &fileSystemConnection = fileSystemEntity.getFileSystemConnection();
	const FileSystemType fileSystemType = fileSystemConnection.getFileSystemType();
	const std::map<std::string, std::string> &connectionProperties = fileSystemConnection.getConnectionProperties();

	EXPECT_TRUE(error.empty());
	EXPECT_NE(fileSystemType, FileSystemType::UNDEFINED);

	switch (fileSystemType) {
		case FileSystemType::HDFS: {
			using namespace HadoopFileSystemConnection;

			const ConnectionProperty property = ConnectionProperty::DRIVER_TYPE;
			const std::string propertyName = connectionPropertyName(property);
			const std::string propertyValue = connectionProperties.at(propertyName);
			const DriverType driverType = driverTypeFromName(propertyValue);
			const bool isLibhdfsDriver = (driverType == DriverType::LIBHDFS);
			const bool isLibhdfs3Driver = (driverType == DriverType::LIBHDFS3);
			const bool correctDriverType = isLibhdfsDriver || isLibhdfs3Driver;

			EXPECT_TRUE(correctDriverType);
			EXPECT_NE(driverType, DriverType::UNDEFINED);
		}
		break;

		case FileSystemType::S3: {
			using namespace S3FileSystemConnection;

			const ConnectionProperty property = ConnectionProperty::ENCRYPTION_TYPE;
			const std::string propertyName = connectionPropertyName(property);
			const std::string propertyValue = connectionProperties.at(propertyName);
			const EncryptionType encryptionType = encryptionTypeFromName(propertyValue);
			const bool isEncryptionNone = (encryptionType == EncryptionType::NONE);
			const bool isEncryptionAes256 = (encryptionType == EncryptionType::AES_256);
			const bool isEncryptionAwsKms = (encryptionType == EncryptionType::AWS_KMS);
			const bool correctEncryptionType = isEncryptionNone || isEncryptionAes256 || isEncryptionAwsKms;

			EXPECT_TRUE(correctEncryptionType);
			EXPECT_NE(encryptionType, EncryptionType::UNDEFINED);
		}
		break;
	}

	EXPECT_EQ(fileSystemEntity, expectedResult);
}

TEST_P(ParseInvalidRegisterFileSystemCommandTest, CheckParseInvalidRegisterFileSystemCommand) {
	std::string error;
	const FileSystemEntity fileSystemEntity = FileSystemCommandParser::parseRegisterFileSystem(command, error);

	EXPECT_FALSE(error.empty());
	EXPECT_EQ(fileSystemEntity.getAuthority(), std::string());
	EXPECT_EQ(fileSystemEntity.getFileSystemConnection().getFileSystemType(), FileSystemType::UNDEFINED);
	EXPECT_TRUE(fileSystemEntity.getFileSystemConnection().getConnectionProperties().empty());
	EXPECT_EQ(fileSystemEntity.getRoot(), Path());
	EXPECT_EQ(fileSystemEntity.getEncryptedAuthority(), std::string());
	EXPECT_EQ(fileSystemEntity.getEncryptedFileSystemConnection(), std::string());
	EXPECT_EQ(fileSystemEntity.getEncryptedRoot(), std::string());
}

INSTANTIATE_TEST_CASE_P(ParseValidRegisterFileSystemCommandTestCase, ParseValidRegisterFileSystemCommandTest, testing::ValuesIn(validRegisterFileSystemCommands));
INSTANTIATE_TEST_CASE_P(ParseInvalidRegisterFileSystemCommandTestCase, ParseInvalidRegisterFileSystemCommandTest, testing::ValuesIn(invalidRegisterFileSystemCommands));
