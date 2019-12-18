#include "Library/Logging/BlazingLogger.h"
#include "Library/Logging/LoggingLevel.h"
#include "Library/Logging/ServiceLogging.h"
#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <regex>
#include <thread>
#include <vector>

using testing::MatchesRegex;
using testing::SafeMatcherCast;
using testing::StrEq;

struct BlazingLoggerTest : public ::testing::Test {
	BlazingLoggerTest() {}

	virtual ~BlazingLoggerTest() {}

	virtual void SetUp() { regex = R"(^\[[0-9]{4}-([0-9]{2})-([0-9]{2})T([0-9]{2}):([0-9]{2}):([0-9]{2})Z\])"; }

	virtual void TearDown() {}

	std::string regex;
	const std::string logData{"sample logging data"};
};

Library::Logging::BlazingLogger Logger() { return Library::Logging::BlazingLogger(); }


TEST_F(BlazingLoggerTest, CheckLogger) {
	auto & mock = Library::Logging::ServiceLogging::getInstance().getMock();
	EXPECT_CALL(mock, setLogData(SafeMatcherCast<const std::string &>(StrEq(logData)))).Times(4);

	{
		Logger().log(logData);
		Logger().log("sample logging data");
	}
	{
		Library::Logging::BlazingLogger logger;
		logger.log(logData);
		logger.log("sample logging data");
	}
}


TEST_F(BlazingLoggerTest, CheckLoggerInfo) {
	auto level = Library::Logging::LoggingLevel::INFO;
	regex += R"(\[)" + std::string(Library::Logging::getLevelName(level)) + R"(\])";
	regex += logData + R"($)";

	auto & mock = Library::Logging::ServiceLogging::getInstance().getMock();
	EXPECT_CALL(mock, setLogData(SafeMatcherCast<const std::string &>(MatchesRegex(regex)))).Times(4);

	{
		Logger().logInfo(logData);
		Logger().logInfo("sample logging data");
	}
	{
		Library::Logging::BlazingLogger logger;
		logger.logInfo(logData);
		logger.logInfo("sample logging data");
	}
}


TEST_F(BlazingLoggerTest, CheckLoggerWarn) {
	auto level = Library::Logging::LoggingLevel::WARN;
	regex += R"(\[)" + std::string(Library::Logging::getLevelName(level)) + R"(\])";
	regex += logData + R"($)";

	auto & mock = Library::Logging::ServiceLogging::getInstance().getMock();
	EXPECT_CALL(mock, setLogData(SafeMatcherCast<const std::string &>(MatchesRegex(regex)))).Times(4);

	{
		Logger().logWarn(logData);
		Logger().logWarn("sample logging data");
	}
	{
		Library::Logging::BlazingLogger logger;
		logger.logWarn(logData);
		logger.logWarn("sample logging data");
	}
}


TEST_F(BlazingLoggerTest, CheckLoggerTrace) {
	auto level = Library::Logging::LoggingLevel::TRACE;
	regex += R"(\[)" + std::string(Library::Logging::getLevelName(level)) + R"(\])";
	regex += logData + R"($)";

	auto & mock = Library::Logging::ServiceLogging::getInstance().getMock();
	EXPECT_CALL(mock, setLogData(SafeMatcherCast<const std::string &>(MatchesRegex(regex)))).Times(4);

	{
		Logger().logTrace(logData);
		Logger().logTrace("sample logging data");
	}
	{
		Library::Logging::BlazingLogger logger;
		logger.logTrace(logData);
		logger.logTrace("sample logging data");
	}
}


TEST_F(BlazingLoggerTest, CheckLoggerDebug) {
	auto level = Library::Logging::LoggingLevel::DEBUG;
	regex += R"(\[)" + std::string(Library::Logging::getLevelName(level)) + R"(\])";
	regex += logData + R"($)";

	auto & mock = Library::Logging::ServiceLogging::getInstance().getMock();
	EXPECT_CALL(mock, setLogData(SafeMatcherCast<const std::string &>(MatchesRegex(regex)))).Times(4);

	{
		Logger().logDebug(logData);
		Logger().logDebug("sample logging data");
	}
	{
		Library::Logging::BlazingLogger logger;
		logger.logDebug(logData);
		logger.logDebug("sample logging data");
	}
}


TEST_F(BlazingLoggerTest, CheckLoggerError) {
	auto level = Library::Logging::LoggingLevel::ERROR;
	regex += R"(\[)" + std::string(Library::Logging::getLevelName(level)) + R"(\])";
	regex += logData + R"($)";

	auto & mock = Library::Logging::ServiceLogging::getInstance().getMock();
	EXPECT_CALL(mock, setLogData(SafeMatcherCast<const std::string &>(MatchesRegex(regex)))).Times(4);

	{
		Logger().logError(logData);
		Logger().logError("sample logging data");
	}
	{
		Library::Logging::BlazingLogger logger;
		logger.logError(logData);
		logger.logError("sample logging data");
	}
}


TEST_F(BlazingLoggerTest, CheckLoggerFatal) {
	auto level = Library::Logging::LoggingLevel::FATAL;
	regex += R"(\[)" + std::string(Library::Logging::getLevelName(level)) + R"(\])";
	regex += logData + R"($)";

	auto & mock = Library::Logging::ServiceLogging::getInstance().getMock();
	EXPECT_CALL(mock, setLogData(SafeMatcherCast<const std::string &>(MatchesRegex(regex)))).Times(4);

	{
		Logger().logFatal(logData);
		Logger().logFatal("sample logging data");
	}
	{
		Library::Logging::BlazingLogger logger;
		logger.logFatal(logData);
		logger.logFatal("sample logging data");
	}
}


TEST_F(BlazingLoggerTest, CheckLoggerMultiThread) {
	const int SIZE = 10;

	auto & mock = Library::Logging::ServiceLogging::getInstance().getMock();
	EXPECT_CALL(mock, setLogData(SafeMatcherCast<const std::string &>(StrEq(logData)))).Times(SIZE);

	std::vector<std::thread> threads;

	for(int i = 0; i < SIZE; ++i) {
		threads.push_back(std::thread([this]() { Logger().log(logData); }));
	}

	for(int i = 0; i < SIZE; ++i) {
		threads[i].join();
	}
}
