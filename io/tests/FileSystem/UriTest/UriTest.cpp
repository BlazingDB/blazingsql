#include <iostream>
#include <limits.h>
#include <time.h>

#include "gtest/gtest.h"

#include "FileSystem/Uri.h"

// TODO import test data from PathTest
static const std::vector<std::string> validDefaultUris = {"/",
	"/a/b/c/v/b/n/m/",
	"/a/b/c/v/b/n",
	"/root/blazing/data.1.parquet",
	"/root/blazing/data_1.simpatico",
	"/mnt/netdisck/blazing/data_1.simpatico",
	"/home/user34/dat.csv",
	"/home/user_45.g3/Desktop/fodlers/this spaced folder/",
	"/home/user_45.g3/Desktop/fodlers/this spaced folder/has_a_file.txt",
	"/home/user_45.g3///Desktop/fodlers///this spaced folder//has_a_file.txt"};

static const std::vector<std::string> validLocalUris = {
	"file://default/home/percy/Desktop/cat_images/",
};

static const std::vector<std::string> validHdfsUris = {
	"hdfs://percy_node_1_spark_warehouse/temp/", "hdfs://percy_node_1_spark_warehouse/temp/"};

static const std::vector<std::string> validS3Uris = {"s3://william_node/root/"};

static std::vector<std::string> getValidUris() {
	std::vector<std::string> result;
	result.insert(result.end(), validLocalUris.begin(), validLocalUris.end());
	result.insert(result.end(), validHdfsUris.begin(), validHdfsUris.end());
	result.insert(result.end(), validS3Uris.begin(), validS3Uris.end());

	return result;
}

const std::vector<std::string> validUris = getValidUris();

// validDefaultUris + validLocalUris + validHdfsUris + validS3Uris;

static const std::vector<std::string> invalidUris = {
	"",
	"cat",
	"../../",
	".",
	"..",
	"/data/../../relative",
	"/home/user_45.g3/Desktop/fodlers/this spaced folder/../has_a_file.txt"
	"hdfs1://some:namespace/rootfolder/path_1/file_2.txt",
	"://some:namespace/rootfolder/path_1/file_2.txt",
	"fil e://some:namespace/rootfolder/path_1/file_2.txt",
	"s3://some:namespace/rootfolder/path_1/../file_2.txt",
	"hdfs:// /rootfolder/path_1/../file_2.txt",
	"hdfs://INVALID_AUTH _A/rootfolder/path_1/../file_2.txt",
};

class UriTest : public testing::TestWithParam<std::string> {
protected:
	virtual void TearDown() {}

protected:
	Uri uri;
};

class UriStringConstructorTest : public UriTest {
protected:
	virtual void SetUp() { uri = Uri(GetParam(), true); }
};

class UriStringNonStrictConstructorTest : public UriTest {
protected:
	virtual void SetUp() { uri = Uri(GetParam(), false); }
};

class UriStringAssignmentTest : public UriTest {
protected:
	virtual void SetUp() { uri = GetParam(); }
};

class ValidDefaultUriStringConstructorTest : public UriStringConstructorTest {};
class ValidDefaultUriStringNonStrictConstructorTest : public UriStringNonStrictConstructorTest {};
class ValidDefaultUriStringAssignmentTest : public UriStringAssignmentTest {};

class ValidUriStringConstructorTest : public UriStringConstructorTest {};
class ValidUriStringNonStrictConstructorTest : public UriStringNonStrictConstructorTest {};
class ValidUriStringAssignmentTest : public UriStringAssignmentTest {};

class InvalidUriStringConstructorTest : public UriStringConstructorTest {};
class InvalidUriStringNonStrictConstructorTest : public UriStringNonStrictConstructorTest {};
class InvalidUriStringAssignmentTest : public UriStringAssignmentTest {};

void checkValidDefaultUriTest(const Uri & uri, const std::string & param) {
	const Uri other(param, true);

	EXPECT_TRUE(uri.isValid());
	EXPECT_FALSE(uri.isEmpty());
	// TODO percy add global static const Uri::DEFAULT_FILE_SYSTEM_TYPE
	EXPECT_EQ(uri.getFileSystemType(), FileSystemType::LOCAL);
	EXPECT_EQ(uri.getScheme(), Uri::fileSystemTypeToScheme(uri.getFileSystemType()));
	EXPECT_EQ(uri.getAuthority(), "local");
	EXPECT_EQ(uri, other);
	EXPECT_EQ(uri.toString(), other.toString());
}

void checkValidUriTest(const Uri & uri, const std::string & param) {
	EXPECT_TRUE(uri.isValid());
	EXPECT_FALSE(uri.isEmpty());
	EXPECT_EQ(uri.toString(), param);
}

void checkInvalidTest(const Uri & uri) { EXPECT_FALSE(uri.isValid()); }

TEST_P(ValidDefaultUriStringConstructorTest, CheckValidDefaultUrisUsingStringConstructor) {
	checkValidDefaultUriTest(uri, GetParam());
}

TEST_P(ValidDefaultUriStringNonStrictConstructorTest, CheckValidDefaultUrisUsingStringNonStrictConstructor) {
	checkValidDefaultUriTest(uri, GetParam());
}

TEST_P(ValidDefaultUriStringAssignmentTest, CheckValidDefaultUrisUsingStringAssignment) {
	checkValidDefaultUriTest(uri, GetParam());
}

TEST_P(ValidUriStringConstructorTest, CheckValidUrisUsingStringConstructor) { checkValidUriTest(uri, GetParam()); }

TEST_P(ValidUriStringNonStrictConstructorTest, CheckValidUrisUsingStringNonStrictConstructor) {
	checkValidUriTest(uri, GetParam());
}

TEST_P(ValidUriStringAssignmentTest, CheckValidUrisUsingStringAssignment) { checkValidUriTest(uri, GetParam()); }

TEST_P(InvalidUriStringConstructorTest, CheckInvalidUrisUsingStringConstructor) { checkInvalidTest(uri); }

TEST_P(InvalidUriStringNonStrictConstructorTest, CheckInvalidUrisUsingStringNonStrictConstructor) {
	checkInvalidTest(uri);
}

TEST_P(InvalidUriStringAssignmentTest, CheckInvalidUrisUsingStringAssignment) { checkInvalidTest(uri); }

INSTANTIATE_TEST_CASE_P(ValidDefaultUrisForStringConstructorTest,
	ValidDefaultUriStringConstructorTest,
	testing::ValuesIn(validDefaultUris));
INSTANTIATE_TEST_CASE_P(ValidDefaultUrisForStringNonStrictConstructorTest,
	ValidDefaultUriStringNonStrictConstructorTest,
	testing::ValuesIn(validDefaultUris));
INSTANTIATE_TEST_CASE_P(
	ValidDefaultUrisForStringAssignmentTest, ValidDefaultUriStringAssignmentTest, testing::ValuesIn(validDefaultUris));

INSTANTIATE_TEST_CASE_P(ValidUrisForStringConstructorTest, ValidUriStringConstructorTest, testing::ValuesIn(validUris));
INSTANTIATE_TEST_CASE_P(
	ValidUrisForStringNonStrictConstructorTest, ValidUriStringNonStrictConstructorTest, testing::ValuesIn(validUris));
INSTANTIATE_TEST_CASE_P(ValidUrisForStringAssignmentTest, ValidUriStringAssignmentTest, testing::ValuesIn(validUris));

INSTANTIATE_TEST_CASE_P(
	InvalidUrisForStringConstructorTest, InvalidUriStringConstructorTest, testing::ValuesIn(invalidUris));
INSTANTIATE_TEST_CASE_P(InvalidUrisForStringNonStrictConstructorTest,
	InvalidUriStringNonStrictConstructorTest,
	testing::ValuesIn(invalidUris));
INSTANTIATE_TEST_CASE_P(
	InvalidUrisStringAssignmentTest, InvalidUriStringAssignmentTest, testing::ValuesIn(invalidUris));
