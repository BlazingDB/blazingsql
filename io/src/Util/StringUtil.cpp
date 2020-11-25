/*
 * StringUtil.cpp
 *
 *  Created on: Aug 7, 2014
 *      Author: felipe
 */

#include "StringUtil.h"

#include <algorithm>
#include <cstring>
#include <limits.h>
#include <sstream>


/**
 * Used to do wild card matches between two strings p and c shoudl be 0 when this is first called
 */
bool match(const char * pattern, const char * candidate, int p, int c) {
	if(pattern[p] == '\0') {
		return candidate[c] == '\0';
	} else if(pattern[p] == '*') {
		for(; candidate[c] != '\0'; c++) {
			if(match(pattern, candidate, p + 1, c))
				return true;
		}
		return match(pattern, candidate, p + 1, c);
	} else if(pattern[p] != '?' && pattern[p] != candidate[c]) {
		return false;
	} else {
		return match(pattern, candidate, p + 1, c + 1);
	}
}


void StringUtil::findAndReplaceAll(std::string & data, std::string toSearch, std::string replaceStr) {
	// Get the first occurrence
	size_t pos = data.find(toSearch);

	// Repeat till end is reached
	while(pos != std::string::npos) {
		// Replace this occurrence of Sub String
		data.replace(pos, toSearch.size(), replaceStr);
		// Get the next occurrence from the current position
		pos = data.find(toSearch, pos + toSearch.size());
	}
}


/**
 * shortcut to call match(const char *, const char *, 0 , 0)
 */
bool match(const char * pattern, const char * candidate) { return match(pattern, candidate, 0, 0); }

// DEPRECATED percy why we need this? ... use URI, Path semantics
std::string removeFileNamespace(const std::string & uri) {
	int colonPosition = uri.find(":");

	return uri.substr(colonPosition + 3, uri.size() - (colonPosition + 3));
}

StringUtil::StringUtil() {
	// TODO Auto-generated constructor stub
}

StringUtil::~StringUtil() {
	// TODO Auto-generated destructor stub
}

std::string randomString(size_t length) {
	auto randchar = []() -> char {
		const char charset[] =
			"0123456789"
			"ABCDEFGHIJKLMNOPQRSTUVWXYZ"
			"abcdefghijklmnopqrstuvwxyz";
		const size_t max_index = (sizeof(charset) - 1);
		return charset[std::rand() % max_index];
	};
	std::string str(length, 0);
	std::generate_n(str.begin(), length, randchar);
	return str;
}

std::string StringUtil::removeEncapsulation(std::string s, std::string encapsulation) {
	int encapSize = encapsulation.size();
	if(s.substr(0, encapSize) == encapsulation && s.substr(s.size() - encapSize, encapSize) == encapsulation) {
		s.erase(0, encapSize);
		s.erase(s.size() - encapSize, encapSize);
	}
	return s;
}

std::string StringUtil::removeEncapsulation(std::string s, std::vector<std::string> encapsulationPair) {
	if(encapsulationPair.size() == 2) {
		if(s.substr(0, encapsulationPair[0].size()) == encapsulationPair[0] &&
			s.substr(s.size() - encapsulationPair[1].size(), encapsulationPair[1].size()) == encapsulationPair[1]) {
			s.erase(0, encapsulationPair[0].size());
			s.erase(s.size() - encapsulationPair[1].size(), encapsulationPair[1].size());
		}
	}
	return s;
}

std::vector<std::string> StringUtil::split(std::string input, std::string regex) {
	// passing -1 as the submatch index parameter performs splitting
	size_t pos = 0;
	std::vector<std::string> result;
	while((pos = input.find(regex)) != std::string::npos) {
		std::string token;
		token = input.substr(0, pos);
		result.push_back(token);
		//  std::cout << token << std::endl;
		input.erase(0, pos + regex.length());
	}
	result.push_back(input);
	return result;
	// std::cout << s << std::endl;
}

// DORU_TESTING
std::vector<std::string> StringUtil::split(std::string & s, char delim) {
	std::vector<std::string> elems;
	split(s, delim, elems);
	return elems;
}


// DORU_TESTING
std::vector<std::string> & StringUtil::split(std::string & s, char delim, std::vector<std::string> & elems) {
	std::stringstream ss(s);
	std::string item;
	while(std::getline(ss, item, delim)) {
		if(!(item.size() == 1 && item[0] == delim)) {
			elems.push_back(item);
		}
	}
	if(s[s.size() - 1] == delim) {
		elems.push_back("");
	}
	return elems;
}


bool StringUtil::isPositiveInteger(const std::string & s) {
	for(size_t i = 0; i < s.size(); i++) {
		if(!std::isdigit(s[i])) {
			return false;
		}
	}
	// convert the string to a number and check whether is a positive number
	return (std::stoi(s) > 0);
}

// TODO: should search for escaped quotes and stuff like that, change them to bell before hand, then change back after
std::string StringUtil::replaceNonQuotedChar(
	std::string textToSearch, std::string quoteChar, std::string charBeingSought, std::string replacingChar) {
	std::vector<std::string> splitString = StringUtil::split(textToSearch, quoteChar);

	for(size_t i = 0; i < splitString.size(); i += 2) {
		if(i < splitString.size()) {
			splitString[i] = StringUtil::replace(splitString[i], charBeingSought, replacingChar);
		}
	}

	std::string result = "";
	for(size_t i = 0; i < splitString.size(); i++) {
		if((i % 2) == 0) {
			result += splitString[i];
		} else {
			result += quoteChar + splitString[i] + quoteChar;
		}
	}
	return result;
}

std::string StringUtil::replaceNonQuotedChar(std::string textToSearch,
	std::string quoteChar,
	std::vector<std::string> & charBeingSought,
	std::vector<std::string> & replacingChar) {
	std::vector<std::string> splitString = StringUtil::split(textToSearch, quoteChar);

	for(size_t i = 0; i < splitString.size(); i += 2) {
		if(i < splitString.size()) {
			for(size_t j = 0; j < charBeingSought.size(); j++) {
				splitString[i] = StringUtil::replace(splitString[i], charBeingSought[j], replacingChar[j]);
			}
		}
	}

	std::string result = "";
	for(size_t i = 0; i < splitString.size(); i++) {
		if((i % 2) == 0) {
			result += splitString[i];
		} else {
			result += quoteChar + splitString[i] + quoteChar;
		}
	}
	return result;
}

std::string StringUtil::replaceQuotedChar(
	std::string textToSearch, std::string quoteChar, std::string charBeingSought, std::string replacingChar) {
	std::vector<std::string> splitString = StringUtil::split(textToSearch, quoteChar);
	if(splitString.size() == 1) {
		return textToSearch;
	}
	for(size_t i = 1; i < splitString.size(); i += 2) {
		if(i < splitString.size()) {
			splitString[i] = StringUtil::replace(splitString[i], charBeingSought, replacingChar);
		}
	}

	std::string result;
	for(size_t i = 0; i < splitString.size(); i++) {
		if((i % 2) == 0) {
			result += splitString[i];
		} else {
			result += quoteChar + splitString[i] + quoteChar;
		}
	}
	return result;
}


std::vector<std::string> StringUtil::splitNonQuotedKeepDelimiterInVector(
	std::string & input, std::vector<std::string> delimiters, char quoteChar, bool trimWhitespace) {
	return splitNonQuotedKeepDelimiterInVector(input, delimiters, quoteChar, trimWhitespace, false);
}

std::vector<std::string> StringUtil::splitNonQuotedKeepDelimiterInVector(std::string & input,
	std::vector<std::string> delimiters,
	char quoteChar,
	bool trimWhitespace,
	bool respectDelimiterWhitespace) {
	std::vector<bool> quoted(input.size());
	bool inQuote = false;
	for(size_t i = 0; i < quoted.size(); i++) {
		if(input[i] == quoteChar) {
			inQuote = !inQuote;
		}
		quoted[i] = inQuote;
	}

	bool continueSplitting = true;
	std::vector<std::string> splitStringArray;
	int currentPosition = 0;
	std::string delimFound = "";
	// std::cout<<currentPosition<<std::endl;
	while(continueSplitting) {
		int firstDelimiterPosition = 99999999;
		for(auto & delimiter : delimiters) {
            size_t currentDelimiterPosition = input.find(delimiter, currentPosition);

			while(currentDelimiterPosition != std::string::npos) {
				if(!quoted[currentDelimiterPosition]) {
					if(currentDelimiterPosition < static_cast<size_t>(firstDelimiterPosition)) {
						firstDelimiterPosition = currentDelimiterPosition;
						delimFound = delimiter;
					}
					break;
				}
				currentDelimiterPosition = input.find(delimiter, currentDelimiterPosition + delimiter.size());
			}
		}

		if(firstDelimiterPosition != 99999999) {
			// adding string before delimiter
			if(firstDelimiterPosition - (currentPosition) > 0) {
				std::string stringFound = input.substr(currentPosition, firstDelimiterPosition - (currentPosition));
				if(trimWhitespace) {
					stringFound = trim(stringFound);
				}
				if(stringFound.size() > 0) {
					splitStringArray.push_back(stringFound);
				}
			}
			splitStringArray.push_back(delimFound);
			if(respectDelimiterWhitespace && delimFound[delimFound.size() - 1] == ' ') {
				currentPosition = firstDelimiterPosition + delimFound.size() - 1;
			} else {
				currentPosition = firstDelimiterPosition + delimFound.size();
			}

		} else {
			// no more delims just copy the rest of the string
			if(input.size() - (currentPosition) > 0) {
				std::string stringFound = input.substr(currentPosition, input.size() - (currentPosition));
				if(trimWhitespace) {
					stringFound = trim(stringFound);
				}
				if(stringFound.size() > 0) {
					splitStringArray.push_back(stringFound);
				}
			}
			continueSplitting = false;
		}
	}
	return splitStringArray;
}


std::string StringUtil::join(std::vector<std::string> splitString, std::string connector) {
	int stringSize = 0;
	for(auto & i : splitString) {
		stringSize += i.size() + 1;
	}
	stringSize--;
	return StringUtil::join(splitString, connector, stringSize);
}

std::string StringUtil::join(std::vector<std::string> splitString, std::string connector, int /*stringSize*/) {
	// std::string joined(stringSize);
	std::stringstream joined;
	if(splitString.size() > 0) {
		joined << splitString[0];
		for(size_t i = 1; i < splitString.size(); i++) {
			joined << connector << splitString[i];
		}
		return joined.str();
	} else {
		return "";
	}
}

std::vector<std::string> StringUtil::splitJoin(std::string input, std::string regex) {
	// passing -1 as the submatch index parameter performs splitting
	size_t pos = 0;
	std::vector<std::string> result;
	while((pos = input.find(regex)) != std::string::npos) {
		std::string token;
		token = input.substr(0, pos);
		result.push_back(token);
		//  std::cout << token << std::endl;
		input.erase(0, pos + regex.length());
	}
	result.push_back(input);
	return result;
	// std::cout << s << std::endl;
}

std::string StringUtil::NumberToString(unsigned long long Number) { return std::to_string(Number); }

std::string StringUtil::NumberToString(double Number) { return std::to_string(Number); }

std::string StringUtil::NumberToString(float Number) { return std::to_string(Number); }

std::string StringUtil::NumberToString(long long Number) { return std::to_string(Number); }

std::string StringUtil::NumberToString(int Number) {
	return std::to_string(Number);
	//     std::ostringstream ss;
	//    ss << Number;
	//   return ss.str();
}


std::string StringUtil::splice(std::vector<std::string> stringsToCombine, std::string combinationString) {
	std::string combinedString = "";
	for(size_t i = 0; i < stringsToCombine.size(); i++) {
		if(i > 0) {
			combinedString += combinationString;
		}
		combinedString += stringsToCombine[i];
	}
	return combinedString;
}


// DORU_TESTING
// TODO we should not return the same string, there should be just one way to use this API
std::string StringUtil::replace(
	std::string containingString, const std::string toReplace, const std::string replacement) {
	std::string::size_type n = 0;
	while((n = containingString.find(toReplace, n)) != std::string::npos) {
		containingString.replace(n, toReplace.size(), replacement);
		n += replacement.size();
	}

	return containingString;
}

bool StringUtil::contains(std::string & haystack, std::string needle) {
	return haystack.find(needle) != std::string::npos;
}

// bool StringUtil::contains(std::string haystack,std::string needle){
//	if(needle.size() > haystack.size()){
//		return false;
//	}
//
//	for(int i = 0; i <= haystack.size() - needle.size();i++){
//		if(haystack.substr(i,needle.size()) == needle){
//			return true;
//		}
//	}
//	return false;
//
//}

std::string StringUtil::combine(std::vector<std::string> arr, std::string delim) {
	std::string retString = "";
	for(size_t i = 0; i < arr.size(); i++) {
		if(i > 0) {
			retString += delim;
		}
		retString += arr[i];
	}
	return retString;
}

// DORU_TESTING
bool StringUtil::beginsWith(std::string haystack, std::string needle) {
	// std::cout<<haystack<<std::endl;
	if(haystack.substr(0, needle.size()) == needle) {
		return true;
	} else {
		return false;
	}
}

bool StringUtil::endsWith(std::string & haystack, std::string & needle) {
	// std::cout<<haystack<<std::endl;
	if(needle.size() > haystack.size()) {
		return false;
	}
	if(haystack.substr(haystack.size() - needle.size(), needle.size()) == needle) {
		return true;
	} else {
		return false;
	}
}
// DORU_TESTING
// trim from start
std::string & StringUtil::ltrim(std::string & s) {
	s.erase(s.begin(), std::find_if(s.begin(), s.end(), std::not1(std::ptr_fun<int, int>(std::isspace))));
	return s;
}
// DORU_TESTING
// trim from end
std::string & StringUtil::rtrim(std::string & s) {
	s.erase(std::find_if(s.rbegin(), s.rend(), std::not1(std::ptr_fun<int, int>(std::isspace))).base(), s.end());
	return s;
}
// DORU_TESTING
// trim from both ends
std::string & StringUtil::trim(std::string & s) { return ltrim(rtrim(s)); }

std::string StringUtil::trimSpacesAndEnclosingQuotes(std::string s) {
	s = trim(s);
	if(s[0] == '\'' && s[s.size() - 1] == '\'') {
		return s.substr(1, s.size() - 2);
	}
	if(s[0] == '"' && s[s.size() - 1] == '"') {
		return s.substr(1, s.size() - 2);
	}
	return s;
}


bool StringUtil::bothAreSpaces(char lhs, char rhs) { return (lhs == rhs) && (lhs == ' '); }

std::string StringUtil::replaceAllWhiteSpaceWithOneSpace(std::string str) {
	str = StringUtil::replace(str, "\r", " ");
	str = StringUtil::replace(str, "\n", " ");
	str = StringUtil::replace(str, "\t", " ");
	std::string::iterator new_end = std::unique(str.begin(), str.end(), bothAreSpaces);
	str.erase(new_end, str.end());
	return str;
}

std::vector<bool> StringUtil::generateQuotedVector(std::string input) {
	std::vector<bool> quoted(input.size());
	bool inQuote = false;
	bool inDoubleQuote = false;
	int i = 0;
	if(input.size() > 2) {
		if(input[i] == '\'' && !(input[i + 1] == '\'')) {
			inQuote = !inQuote;
		}
		if(input[i] == '"') {
			inDoubleQuote = !inDoubleQuote;
		}
		quoted[i] = inQuote | inDoubleQuote;
		for(size_t j = 1; j < quoted.size() - 1; j++) {
			if(input[j] == '\'' && !(input[j + 1] == '\'' || input[j - 1] == '\'')) {
				inQuote = !inQuote;
			}
			if(input[j] == '"') {
				inDoubleQuote = !inDoubleQuote;
			}
			quoted[j] = inQuote | inDoubleQuote;
		}
		i = quoted.size() - 1;
		if(input[i] == '\'' && !(input[i - 1] == '\'')) {
			inQuote = !inQuote;
		}
		if(input[i] == '"') {
			inDoubleQuote = !inDoubleQuote;
		}
		quoted[i] = inQuote | inDoubleQuote;
	}
	return quoted;
}

int StringUtil::findFirstNotInQuotes(std::string haystack, std::string needle) {
	std::vector<bool> quoted = generateQuotedVector(haystack);
	return findFirstNotInQuotes(haystack, needle, 0, quoted);
}

int StringUtil::findFirstNotInQuotes(
	std::string haystack, std::vector<std::string> needles, std::string & needleFound) {
	std::vector<bool> quoted = generateQuotedVector(haystack);
	return findFirstNotInQuotes(haystack, needles, needleFound, 0, quoted);
}

int StringUtil::findFirstNotInQuotes(std::string haystack, std::string needle, size_t pos, std::vector<bool> & quoted) {
	if(quoted.size() != haystack.size()) {
		quoted = generateQuotedVector(haystack);
	}

	while(pos < haystack.size()) {
		pos = haystack.find(needle, pos);
		if(pos == -1ULL || !quoted[pos]) {
			return pos;
		}
		pos += needle.size();
	}
	return -1;
}

int StringUtil::findFirstNotInQuotes(std::string haystack,
	std::vector<std::string> needles,
	std::string & needleFound,
	int startPos,
	std::vector<bool> & quoted) {
	if(quoted.size() != haystack.size()) {
		quoted = generateQuotedVector(haystack);
	}

	int minPos = INT_MAX;
	needleFound = "";
	for(size_t i = 0; i < needles.size(); i++) {
		int pos = startPos;
		while(pos != -1) {
			pos = haystack.find(needles[i], pos);
			if(pos != -1) {
				if(!quoted[pos] && pos < minPos) {
					minPos = pos;
					needleFound = needles[i];
					break;
				} else {
					pos = pos + needles[i].size();
				}
			}
		}
	}
	if(minPos == INT_MAX) {
		return -1;
	} else {
		return minPos;
	}
}

std::vector<std::string> StringUtil::splitNotInQuotes(std::string input, std::string delim) {
	std::vector<bool> quoted = generateQuotedVector(input);
	return splitNotInQuotes(input, delim, quoted);
}


std::vector<std::string> StringUtil::splitNotInQuotes(
	std::string input, std::string delim, std::vector<bool> & quoted) {
	if(quoted.size() != input.size()) {
		quoted = generateQuotedVector(input);
	}

	std::vector<std::string> elements;
	int delimSize = delim.size();

	int delimPos = -delimSize;
	int newDelimPos = 0;

	while(newDelimPos != -1) {
		newDelimPos = findFirstNotInQuotes(input, delim, delimPos + delimSize, quoted);
		if(newDelimPos != -1) {
			elements.push_back(input.substr(delimPos + delimSize, newDelimPos - (delimPos + delimSize)));
			delimPos = newDelimPos;
		}
	}
	if(static_cast<size_t>(delimPos + delimSize) < input.size()) {
		elements.push_back(input.substr(delimPos + delimSize));
	}
	return elements;
}

std::string StringUtil::toLower(const std::string & input) {
	std::string output;
	output.resize(input.size());
	std::transform(input.begin(), input.end(), output.begin(), ::tolower);
	return output;
}

std::string StringUtil::toUpper(const std::string & input) {
	std::string output;
	output.resize(input.size());
	std::transform(input.begin(), input.end(), output.begin(), ::toupper);
	return output;
}

// matches a string including widcards
bool StringUtil::match(char const * needle, char const * haystack) {
	for(; *needle != '\0'; ++needle) {
		switch(*needle) {
		case '?':
			if(*haystack == '\0')
				return false;
			++haystack;
			break;
		case '*': {
			if(needle[1] == '\0')
				return true;
			int max = strlen(haystack);
			for(int i = 0; i < max; i++)
				if(match(needle + 1, haystack + i))
					return true;
			return false;
		}
		default:
			if(*haystack != *needle)
				return false;
			++haystack;
		}
	}
	return *haystack == '\0';
}
