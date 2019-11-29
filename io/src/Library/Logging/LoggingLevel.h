#ifndef SRC_LIBRARY_LOGGING_LOGGINGLEVEL_H_
#define SRC_LIBRARY_LOGGING_LOGGINGLEVEL_H_

namespace Library {
    namespace Logging {
        enum class LoggingLevel {
            INFO,
            WARN,
            TRACE,
            DEBUG,
            ERROR,
            FATAL
        };

        const char* getLevelName(LoggingLevel level);
    }
}

#endif
