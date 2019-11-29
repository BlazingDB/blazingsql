#ifndef SRC_LIBRARY_LOGGING_BLAZINGLOGGER_H_
#define SRC_LIBRARY_LOGGING_BLAZINGLOGGER_H_

#include <string>
#include "Library/Logging/LoggingLevel.h"

namespace Library {
    namespace Logging {
        class BlazingLogger {
        public:
            BlazingLogger();

            ~BlazingLogger();

            BlazingLogger(BlazingLogger&&);

            BlazingLogger(const BlazingLogger&) = delete;

            BlazingLogger& operator=(BlazingLogger&&) = delete;

            BlazingLogger& operator=(const BlazingLogger&) = delete;

        private:
            void* operator new(size_t) = delete;

            void* operator new(size_t, void*) = delete;

            void* operator new[](size_t) = delete;

            void* operator new[](size_t, void*) = delete;

        public:
            void log(std::string&& logdata);

            void log(const std::string& logdata);

            void logInfo(std::string&& logdata);

            void logInfo(const std::string& logdata);

            void logWarn(std::string&& logdata);

            void logWarn(const std::string& logdata);

            void logTrace(std::string&& logdata);

            void logTrace(const std::string& logdata);

            void logDebug(std::string&& logdata);

            void logDebug(const std::string& logdata);

            void logError(std::string&& logdata);

            void logError(const std::string& logdata);

            void logFatal(std::string&& logdata);

            void logFatal(const std::string& logdata);

        private:
            void buildLogData(LoggingLevel level, const std::string& logdata);

            void sendDataToService(const std::string& logdata);
        };
    }
}

#endif
