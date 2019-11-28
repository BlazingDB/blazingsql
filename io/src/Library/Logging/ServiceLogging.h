#ifndef SRC_LIBRARY_LOGGING_SERVICELOGGING_H_
#define SRC_LIBRARY_LOGGING_SERVICELOGGING_H_

#include <string>

namespace Library {
    namespace Logging {
        class GenericOutput;

        class ServiceLogging {
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

            void setLogData(const std::string& datetime, const std::string& level, const std::string& message);

            void setLogOutput(GenericOutput* output);

            void setNodeIdentifier(const int nodeInd);

        private:
            GenericOutput* output {nullptr};
            int nodeInd;
        };
    }
}

#endif
