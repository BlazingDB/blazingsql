#ifndef TEST_MOCK_LIBRARY_LOGGING_SERVICELOGGING_H_
#define TEST_MOCK_LIBRARY_LOGGING_SERVICELOGGING_H_

#include <gmock/gmock.h>

namespace Library {
    namespace Logging {
        class GenericOutput;
    }
}

namespace BlazingTest {
    namespace Library {
        namespace Logging {
            struct ServiceLogging {
                virtual ~ServiceLogging() {}

                virtual void setLogData(std::string&& data) = 0;

                virtual void setLogData(const std::string& data) = 0;

                virtual void setLogOutput(::Library::Logging::GenericOutput* output) = 0;
            };

            struct ServiceLoggingMock : public ServiceLogging {
                MOCK_METHOD1(setLogData, void(std::string&& data));

                MOCK_METHOD1(setLogData, void(const std::string& data));

                MOCK_METHOD1(setLogOutput, void(::Library::Logging::GenericOutput* output));
            };
        }
    }
}

namespace Library {
    namespace Logging {
        class ServiceLogging {
        public:
            BlazingTest::Library::Logging::ServiceLoggingMock& getMock() {
                return *mock;
            }

        private:
            ServiceLogging();

            ServiceLogging(ServiceLogging&&) = delete;

            ServiceLogging(const ServiceLogging&) = delete;

            ServiceLogging& operator=(ServiceLogging&&) = delete;

            ServiceLogging& operator=(const ServiceLogging&) = delete;

        public:
            ~ServiceLogging();

        public:
            static ServiceLogging& getInstance() {
                static ServiceLogging service;
                return service;
            }

        public:
            void setLogData(std::string&& data);

            void setLogData(const std::string& data);

            void setLogOutput(GenericOutput* output);

        private:
            BlazingTest::Library::Logging::ServiceLoggingMock* mock {nullptr};
        };
    }
}

#endif
