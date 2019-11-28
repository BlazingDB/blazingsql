#include <gtest/gtest.h>
#include <gmock/gmock.h>
#include "Library/Logging/Logger.h"

using testing::StrEq;
using testing::SafeMatcherCast;

TEST(LoggerTest, ReturnBlazingLoggerObject) {
    BlazingTest::Library::Logging::BlazingLoggerMock mock;
    Library::Logging::BlazingLogger::setStaticMock(mock);
    Library::Logging::BlazingLogger::setStaticMockOnMethods();

    const std::string logData {"log data sample"};

    EXPECT_CALL(mock, log(SafeMatcherCast<const std::string&>(StrEq(logData))))
               .Times(1);

    Library::Logging::Logger().log(logData);
}
