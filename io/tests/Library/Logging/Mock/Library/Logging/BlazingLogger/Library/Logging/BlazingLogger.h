#ifndef TEST_MOCK_LIBRARY_LOGGING_BLAZINGLOGGER_H_
#define TEST_MOCK_LIBRARY_LOGGING_BLAZINGLOGGER_H_

#include <gmock/gmock.h>
#include <string>

namespace BlazingTest {
namespace Library {
namespace Logging {
struct BlazingLogger {
	virtual void log(std::string && logdata) = 0;

	virtual void log(const std::string & logdata) = 0;

	virtual void logInfo(std::string && logdata) = 0;

	virtual void logInfo(const std::string & logdata) = 0;

	virtual void logWarn(std::string && logdata) = 0;

	virtual void logWarn(const std::string & logdata) = 0;

	virtual void logTrace(std::string && logdata) = 0;

	virtual void logTrace(const std::string & logdata) = 0;

	virtual void logDebug(std::string && logdata) = 0;

	virtual void logDebug(const std::string & logdata) = 0;

	virtual void logError(std::string && logdata) = 0;

	virtual void logError(const std::string & logdata) = 0;

	virtual void logFatal(std::string && logdata) = 0;

	virtual void logFatal(const std::string & logdata) = 0;
};

struct BlazingLoggerMock : public BlazingLogger {
	MOCK_METHOD1(log, void(std::string &&));

	MOCK_METHOD1(log, void(const std::string &));

	MOCK_METHOD1(logInfo, void(std::string &&));

	MOCK_METHOD1(logInfo, void(const std::string &));

	MOCK_METHOD1(logWarn, void(std::string &&));

	MOCK_METHOD1(logWarn, void(const std::string &));

	MOCK_METHOD1(logTrace, void(std::string &&));

	MOCK_METHOD1(logTrace, void(const std::string &));

	MOCK_METHOD1(logDebug, void(std::string &&));

	MOCK_METHOD1(logDebug, void(const std::string &));

	MOCK_METHOD1(logError, void(std::string &&));

	MOCK_METHOD1(logError, void(const std::string &));

	MOCK_METHOD1(logFatal, void(std::string &&));

	MOCK_METHOD1(logFatal, void(const std::string &));
};
}  // namespace Logging
}  // namespace Library
}  // namespace BlazingTest

namespace Library {
namespace Logging {
class BlazingLogger {
public:
	BlazingTest::Library::Logging::BlazingLoggerMock & getMock() { return mock; }

	static void setStaticMockOnMethods() { useStaticMockOnMethods = true; }

	static void setStaticMock(BlazingTest::Library::Logging::BlazingLoggerMock & value) { staticMock = &value; }

public:
	BlazingLogger();

	~BlazingLogger();

	BlazingLogger(BlazingLogger &&);

	BlazingLogger(const BlazingLogger &) = delete;

	BlazingLogger & operator=(BlazingLogger &&) = delete;

	BlazingLogger & operator=(const BlazingLogger &) = delete;

public:
	explicit BlazingLogger(std::string && logdata);

	explicit BlazingLogger(const std::string & logdata);

private:
	void * operator new(size_t) = delete;

	void * operator new(size_t, void *) = delete;

	void * operator new[](size_t) = delete;

	void * operator new[](size_t, void *) = delete;

public:
	void log(std::string && logdata);

	void log(const std::string & logdata);

	void logInfo(std::string && logdata);

	void logInfo(const std::string & logdata);

	void logWarn(std::string && logdata);

	void logWarn(const std::string & logdata);

	void logTrace(std::string && logdata);

	void logTrace(const std::string & logdata);

	void logDebug(std::string && logdata);

	void logDebug(const std::string & logdata);

	void logError(std::string && logdata);

	void logError(const std::string & logdata);

	void logFatal(std::string && logdata);

	void logFatal(const std::string & logdata);

private:
	BlazingTest::Library::Logging::BlazingLoggerMock mock;

private:
	static bool useStaticMockOnMethods;

	static BlazingTest::Library::Logging::BlazingLoggerMock * staticMock;
};
}  // namespace Logging
}  // namespace Library

#endif
