/**
 * Test cases hierarchy
 *
 *                 +----------------+
 *                 | DeregisterTest |---------+
 *                 +----------------+         |
 *                       |                    |
 *                       |               +----------------------+
 * +-------------------------------+     | WithCommandParameter |
 * | WithRegisteredFileSystemsTest |     +----------------------+
 * +-------------------------------+                |
 *                                          +-------o------+
 *                                          |              |
 *                                 +------------+      +--------------+
 *                                 | ValidParse |      | InvalidParse |
 *                                 +------------+      +--------------+
 */

#include "gtest/gtest.h"

#include "FileSystem/FileSystemCommandParser.h"

class DeregisterTest: public testing::Test {
	protected:
		std::string authority;
		std::string error;
};

class WithCommandParameter: public DeregisterTest, public testing::WithParamInterface<std::string> {
	protected:
		virtual void SetUp() {
			using FileSystemCommandParser::parseDeregisterFileSystem;

			std::string command = GetParam();
			authority = parseDeregisterFileSystem(command, error);
		}
};

// Valid parse

class ValidParse: public WithCommandParameter {
};

TEST_P(ValidParse, Pass) {
	EXPECT_FALSE(authority.empty());
	EXPECT_TRUE(error.empty());
}

INSTANTIATE_TEST_CASE_P(ValidCommand, ValidParse, testing::Values("deregister file system 'test_local_disk'"));

// Invalid parse

class InvalidParse: public WithCommandParameter {
};

TEST_P(InvalidParse, Refuse) {
	EXPECT_TRUE(authority.empty());
	EXPECT_FALSE(error.empty());
}

INSTANTIATE_TEST_CASE_P(InvalidCommand, InvalidParse,
	testing::Values("ill-deregister file system 'test_local_disk'", "deregister files system 'test_local_disk'", "deregister file sys 'test_local_disk'"
//
// TODO: ask for correctness; if so, move to valid (command) parameter list
//
//        "deregister file system 'test_local_disk';"
//        "deregister file system test_local_disk"
//        "deregister file system \"test_local_disk\""
		));

// With registered file systems

class WithRegisteredFileSystemsTest: public DeregisterTest {
	protected:
		virtual void SetUp() {
			using FileSystemCommandParser::parseRegisterFileSystem;

			parseRegisterFileSystem("register local file system stored as 'local_disk1' root '/opt/'", error);
			parseRegisterFileSystem("register local file system stored as 'local_disk2'", error);
		}

		virtual void TearDown() {
			// TODO: avoid calling SUT here!! => side effects on parser
			using FileSystemCommandParser::parseDeregisterFileSystem;

			parseDeregisterFileSystem("deregister file system 'local_disk1'", error);
			parseDeregisterFileSystem("deregister file system 'local_disk2'", error);
		}
};

TEST_F(WithRegisteredFileSystemsTest, GetAuthorityWhenDeregister) {
	authority = FileSystemCommandParser::parseDeregisterFileSystem("deregister file system 'local_disk1'", error);

	EXPECT_FALSE(authority.empty());
	EXPECT_STREQ("local_disk1", authority.c_str());
}
