#include "Library/Logging/BlazingLogger.h"

namespace Library {
    namespace Logging {
        bool BlazingLogger::useStaticMockOnMethods {false};

        BlazingTest::Library::Logging::BlazingLoggerMock* BlazingLogger::staticMock {nullptr};

        BlazingLogger::BlazingLogger() {
        }

        BlazingLogger::~BlazingLogger() {
        }

        BlazingLogger::BlazingLogger(BlazingLogger&&) {
        }

        void BlazingLogger::log(std::string&& logdata) {
            if (!useStaticMockOnMethods) {
                mock.log(std::move(logdata));
            } else {
                staticMock->log(std::move(logdata));
            }
        }

        void BlazingLogger::log(const std::string& logdata) {
            if (!useStaticMockOnMethods) {
                mock.log(logdata);
            } else {
                staticMock->log(logdata);
            }
        }

        void BlazingLogger::logInfo(std::string&& logdata) {
            if (!useStaticMockOnMethods) {
                mock.log(std::move(logdata));
            } else {
                staticMock->log(std::move(logdata));
            }
        }

        void BlazingLogger::logInfo(const std::string& logdata) {
            if (!useStaticMockOnMethods) {
                mock.log(logdata);
            } else {
                staticMock->log(logdata);
            }
        }

        void BlazingLogger::logWarn(std::string&& logdata) {
            if (!useStaticMockOnMethods) {
                mock.log(std::move(logdata));
            } else {
                staticMock->log(std::move(logdata));
            }
        }

        void BlazingLogger::logWarn(const std::string& logdata) {
            if (!useStaticMockOnMethods) {
                mock.log(logdata);
            } else {
                staticMock->log(logdata);
            }
        }

        void BlazingLogger::logTrace(std::string&& logdata) {
            if (!useStaticMockOnMethods) {
                mock.log(std::move(logdata));
            } else {
                staticMock->log(std::move(logdata));
            }
        }

        void BlazingLogger::logTrace(const std::string& logdata) {
            if (!useStaticMockOnMethods) {
                mock.log(logdata);
            } else {
                staticMock->log(logdata);
            }
        }

        void BlazingLogger::logDebug(std::string&& logdata) {
            if (!useStaticMockOnMethods) {
                mock.log(std::move(logdata));
            } else {
                staticMock->log(std::move(logdata));
            }
        }

        void BlazingLogger::logDebug(const std::string& logdata) {
            if (!useStaticMockOnMethods) {
                mock.log(logdata);
            } else {
                staticMock->log(logdata);
            }
        }

        void BlazingLogger::logError(std::string&& logdata) {
            if (!useStaticMockOnMethods) {
                mock.log(std::move(logdata));
            } else {
                staticMock->log(std::move(logdata));
            }
        }

        void BlazingLogger::logError(const std::string& logdata) {
            if (!useStaticMockOnMethods) {
                mock.log(logdata);
            } else {
                staticMock->log(logdata);
            }
        }

        void BlazingLogger::logFatal(std::string&& logdata) {
            if (!useStaticMockOnMethods) {
                mock.log(std::move(logdata));
            } else {
                staticMock->log(std::move(logdata));
            }
        }

        void BlazingLogger::logFatal(const std::string& logdata) {
            if (!useStaticMockOnMethods) {
                mock.log(logdata);
            } else {
                staticMock->log(logdata);
            }
        }
    }
}
