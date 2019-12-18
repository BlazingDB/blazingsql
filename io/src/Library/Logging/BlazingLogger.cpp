#include "Library/Logging/BlazingLogger.h"
#include "Library/Logging/ServiceLogging.h"
#include <chrono>
#include <iomanip>
#include <sstream>

namespace Library {
namespace Logging {
BlazingLogger::BlazingLogger() {}

BlazingLogger::~BlazingLogger() {}

BlazingLogger::BlazingLogger(BlazingLogger &&) {}

void BlazingLogger::log(std::string && logdata) { sendDataToService(logdata); }

void BlazingLogger::log(const std::string & logdata) { sendDataToService(logdata); }

void BlazingLogger::logInfo(std::string && logdata) { buildLogData(LoggingLevel::INFO, logdata); }

void BlazingLogger::logInfo(const std::string & logdata) { buildLogData(LoggingLevel::INFO, logdata); }

void BlazingLogger::logWarn(std::string && logdata) { buildLogData(LoggingLevel::WARN, logdata); }

void BlazingLogger::logWarn(const std::string & logdata) { buildLogData(LoggingLevel::WARN, logdata); }

void BlazingLogger::logTrace(std::string && logdata) { buildLogData(LoggingLevel::TRACE, logdata); }

void BlazingLogger::logTrace(const std::string & logdata) { buildLogData(LoggingLevel::TRACE, logdata); }

void BlazingLogger::logDebug(std::string && logdata) { buildLogData(LoggingLevel::DEBUG, logdata); }

void BlazingLogger::logDebug(const std::string & logdata) { buildLogData(LoggingLevel::DEBUG, logdata); }

void BlazingLogger::logError(std::string && logdata) { buildLogData(LoggingLevel::ERROR, logdata); }

void BlazingLogger::logError(const std::string & logdata) { buildLogData(LoggingLevel::ERROR, logdata); }

void BlazingLogger::logFatal(std::string && logdata) { buildLogData(LoggingLevel::FATAL, logdata); }

void BlazingLogger::logFatal(const std::string & logdata) { buildLogData(LoggingLevel::FATAL, logdata); }

void BlazingLogger::buildLogData(LoggingLevel level, const std::string & logdata) {
	auto now = std::chrono::system_clock::now();
	auto time = std::chrono::system_clock::to_time_t(now);

	std::ostringstream ss;
	ss << std::put_time(std::localtime(&time), "%FT%TZ");
	ServiceLogging::getInstance().setLogData(ss.str(), getLevelName(level), logdata);
}

void BlazingLogger::sendDataToService(const std::string & logdata) {
	ServiceLogging::getInstance().setLogData(logdata);
}
}  // namespace Logging
}  // namespace Library
