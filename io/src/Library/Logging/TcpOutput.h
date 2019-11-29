#ifndef SRC_LIBRARY_LOGGING_TCPOUTPUT_H_
#define SRC_LIBRARY_LOGGING_TCPOUTPUT_H_

#include <deque>
#include <chrono>
#include <thread>
#include <atomic>
#include <mutex>
#include <condition_variable>
#include "Library/Logging/GenericOutput.h"
#include "Library/Network/GenericSocket.h"

namespace Library {
    namespace Logging {
        class TcpOutput : public GenericOutput {
        public:
            TcpOutput();

            ~TcpOutput();

        public:
            TcpOutput(TcpOutput&&) = delete;

            TcpOutput(const TcpOutput&) = delete;

            TcpOutput& operator=(TcpOutput&&) = delete;

            TcpOutput& operator=(const TcpOutput&) = delete;

        public:
            void setMaxBufferSize(int value);

            void setWaitTime(std::chrono::milliseconds&& value);

            void setWaitTime(const std::chrono::milliseconds& value);

            void setSocket(std::shared_ptr<Library::Network::GenericSocket>& value);

        public:
            void start();

            void stop();

        public:
            void flush(std::string&& log) override;

            void flush(const std::string& log) override;

            void flush(const int nodeInd, const std::string& datetime, const std::string& level, const std::string& log) override;

            // void setNodeIdentifier(const unsigned int nodeInd) override;

        private:
            void doOnProducer(const std::string& log);

            void doOnConsumer();

            void sendLogData();

            void processLogData();

        private:
            volatile bool isActive;
            volatile bool isReady;

        private:
            std::atomic<bool> isDataReadyToSend;

        private:
            std::mutex mutex;
            std::thread thread;
            std::condition_variable condition;
            std::chrono::milliseconds waitTime;
            // unsigned int nodeInd;

        private:
            std::string buffer;
            std::deque<std::string> deque;

        private:
            int maxBufferSize;
            std::shared_ptr<Library::Network::GenericSocket> socket;
        };
    }
}

#endif
