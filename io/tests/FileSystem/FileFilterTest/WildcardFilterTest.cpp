#include "gtest/gtest.h"

#include "FileSystem/FileFilter.h"

struct MatchWildcardFilterTestParam {
	MatchWildcardFilterTestParam(const std::string &input, const std::string &wildcard, bool match)
		: input(input)
		, wildcard(wildcard)
		, match(match){
	}

	std::string input;
	std::string wildcard;
	bool match;
};

class MatchWildcardFilterTest : public testing::TestWithParam<MatchWildcardFilterTestParam> {
	protected:
		virtual void SetUp() {
			const MatchWildcardFilterTestParam param = GetParam(); // test parameter
			this->input = param.input;
			this->wildcard = param.wildcard;
			this->match = param.match;
		}

		virtual void TearDown() {}

	protected:
		std::string input;
		std::string wildcard;
		bool match;
};

const std::vector<MatchWildcardFilterTestParam> matchWildcardInputs = {
	MatchWildcardFilterTestParam(
		"/percy/camilo/data.txt",
		"*.txt",
		true
	),
	MatchWildcardFilterTestParam(
		"/percy/camilo/data.txt",
		"*data*",
		true
	),
	MatchWildcardFilterTestParam(
		"/percy/camilo/data.txt",
		"i*",
		false
	),
	MatchWildcardFilterTestParam(
		"/blazingdb/rocks/./data.txt",
		"*",
		true
	),
	MatchWildcardFilterTestParam(
		"/blazingdb/rocks/./data.txt",
		"*simply",
		false
	),
	MatchWildcardFilterTestParam(
		"//usr/share/folders/meta.data/data.txt",
		"*.data*",
		true
	)
};

TEST_P(MatchWildcardFilterTest, CheckAllMatch) {
	const bool result = WildcardFilter::match(input, wildcard);
	EXPECT_EQ(result, match);
}

INSTANTIATE_TEST_CASE_P(MatchWildcardFilterTestCase, MatchWildcardFilterTest, testing::ValuesIn(matchWildcardInputs));
