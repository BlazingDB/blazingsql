#include "Library/Logging/LoggingLevel.h"

namespace Library {
namespace Logging {
const char * getLevelName(LoggingLevel level) {
	switch(level) {
	case LoggingLevel::INFO: return "INFO";
	case LoggingLevel::WARN: return "WARN";
	case LoggingLevel::TRACE: return "TRACE";
	case LoggingLevel::DEBUG: return "DEBUG";
	case LoggingLevel::ERROR: return "ERROR";
	case LoggingLevel::FATAL: return "FATAL";
	}
	return "";
}
}  // namespace Logging
}  // namespace Library
