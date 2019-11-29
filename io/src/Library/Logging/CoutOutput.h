#ifndef SRC_LIBRARY_LOGGING_COUTOUTPUT_H_
#define SRC_LIBRARY_LOGGING_COUTOUTPUT_H_

#include "Library/Logging/GenericOutput.h"
#include <mutex>

namespace Library {
    namespace Logging {
        class CoutOutput : public GenericOutput {
        public:
            CoutOutput();

            ~CoutOutput();

        public:
            CoutOutput(CoutOutput&&) = delete;

            CoutOutput(const CoutOutput&) = delete;

            CoutOutput& operator=(CoutOutput&&) = delete;

            CoutOutput& operator=(const CoutOutput&) = delete;

        public:
            void flush(std::string&& log) override;

            void flush(const std::string& log) override;

            void flush(const int nodeInd, const std::string& datetime, const std::string& level, const std::string& log) override;

            // void setNodeIdentifier(const unsigned int nodeInd) override;

        private:
            std::mutex mutex;
        };
    }
}

#endif
