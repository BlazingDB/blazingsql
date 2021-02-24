/*
 * StringUtil.h
 *
 *  Created on: Aug 7, 2014
 *      Author: felipe
 */

#ifndef STRINGUTIL_H_
#define STRINGUTIL_H_

#include <algorithm>
#include <string>
#include <vector>
std::string randomString(size_t length);

class StringUtil {
public:
	StringUtil();
	virtual ~StringUtil();
	static std::string & ltrim(std::string & s);
	static std::string & rtrim(std::string & s);
	static std::string & trim(std::string & s);
	static std::string trimSpacesAndEnclosingQuotes(std::string s);
	static bool bothAreSpaces(char lhs, char rhs);
	static bool endsWith(std::string & haystack, std::string & needle);
	static std::string replaceAllWhiteSpaceWithOneSpace(std::string);
	static std::vector<std::string> & split(std::string & s, char delim, std::vector<std::string> & elems);
	static std::vector<std::string> & split(std::string & s, std::string delim, std::vector<std::string> & elems);
	static std::vector<std::string> splitNonQuotedKeepDelimiterInVector(
		std::string & input, std::vector<std::string> delimiters, char quoteChar, bool trimWhitespace);
	static std::vector<std::string> splitNonQuotedKeepDelimiterInVector(std::string & input,
		std::vector<std::string> delimiters,
		char quoteChar,
		bool trimWhitespace,
		bool respectDelimiterWhitespace);
	static bool isPositiveInteger(const std::string &);
	static std::string replaceNonQuotedChar(
		std::string textToSearch, std::string quoteChar, std::string charBeingSought, std::string replacingChar);
	static std::string replaceNonQuotedChar(std::string textToSearch,
		std::string quoteChar,
		std::vector<std::string> & charBeingSought,
		std::vector<std::string> & replacingChar);
	static std::string removeEncapsulation(std::string s, std::string encapsulation);
	static std::string removeEncapsulation(std::string s, std::vector<std::string> encapsulation);
	static std::vector<std::string> split(std::string & s, char delim);
	static std::vector<std::string> split(std::string s, std::string delim);
	static std::vector<std::string> splitJoin(std::string s, std::string delim);
	static std::vector<std::string> split(char *, char delim);
	static std::string combine(std::vector<std::string>, std::string);
	static std::string replace(std::string, const std::string, const std::string);
	static std::string NumberToString(long long);
	static std::string NumberToString(unsigned long long);
	static std::string NumberToString(float);
	static std::string NumberToString(double);

	static std::string replaceQuotedChar(
		std::string textToSearch, std::string quoteChar, std::string charBeingSought, std::string replacingChar);
	static std::string join(std::vector<std::string> splitString, std::string connector);
	static std::string join(std::vector<std::string> splitString, std::string connector, int stringSize);

	static std::string NumberToString(int);
	static std::string splice(std::vector<std::string> stringsToCombine, std::string combinationString);

	static bool beginsWith(std::string haystack, std::string needle);
	//	static bool contains(std::string haystack,std::string needle);
	static bool contains(std::string & haystack, std::string needle);
	static std::vector<bool> generateQuotedVector(std::string input);
	static int findFirstNotInQuotes(std::string haystack, std::string needle);
	static int findFirstNotInQuotes(std::string haystack, std::string needle, size_t pos, std::vector<bool> & quoted);
	static int findFirstNotInQuotes(std::string haystack, std::vector<std::string> needles, std::string & needleFound);
	static int findFirstNotInQuotes(std::string haystack,
		std::vector<std::string> needles,
		std::string & needleFound,
		int startPos,
		std::vector<bool> & quoted);
	static std::vector<std::string> splitNotInQuotes(std::string input, std::string delim);
	static std::vector<std::string> splitNotInQuotes(std::string input, std::string delim, std::vector<bool> & quoted);
	static std::string toLower(const std::string & input);
	static std::string toUpper(const std::string & input);
	static bool match(std::string & needle, std::string & haystack) { return match(needle.c_str(), haystack.c_str()); }
	static bool match(char const * needle, char const * haystack);
	static void findAndReplaceAll(std::string & data, std::string toSearch, std::string replaceStr);
	static size_t findAndCountAllMatches(const std::string & input, std::string word);
	static std::string makeCommaDelimitedSequence(std::size_t n_cols);
};

/**
 * Used to do wild card matches between two strings p and c shoudl be 0 when this is first called
 */
bool match(const char * pattern, const char * candidate, int p, int c);
/**
 * shortcut to call match(const char *, const char *, 0 , 0)
 */
bool match(const char * pattern, const char * candidate);

std::string removeFileNamespace(const std::string & uri);

#endif /* STRINGUTIL_H_ */
