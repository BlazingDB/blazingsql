#include <gtest/gtest.h>
#include <gmock/gmock.h>
#include <vector>
#include <chrono>
#include <thread>
#include <sstream>
#include "GenericSocketMock.h"
#include "Library/Logging/TcpOutput.h"

using testing::_;
using testing::WithArg;
using testing::Invoke;
using testing::SafeMatcherCast;

using namespace std::chrono_literals;

struct Generator {
    Generator(unsigned int id, unsigned int times, const std::string& data)
    : id{id}, times{times}, data{data} {
    }

    Generator(Generator&& generator)
    : id{generator.id}, times{generator.times}, data{generator.data} {
    }

    void operator()(Library::Logging::GenericOutput* output) {
        std::stringstream ss;
        for (unsigned int k = 0; k < times; ++k) {
            ss.str("");
            ss.clear();
            ss << id << ' ' << k << ' ' << data;
            output->flush(ss.str());
        }
    }

    const unsigned int id;
    const unsigned int times;
    const std::string data;
};

struct TcpOutputTest : public ::testing::Test {
    TcpOutputTest() {
    }

    virtual ~TcpOutputTest() {
    }

    virtual void SetUp() {
        mock = std::make_shared<BlazingTest::Library::Network::GenericSocketMock>();

        output = new Library::Logging::TcpOutput();
        tcpOutput = dynamic_cast<::Library::Logging::TcpOutput*>(output);

        tcpOutput->setSocket(mock);
        tcpOutput->setWaitTime(waitTime);
        tcpOutput->setMaxBufferSize(bufferSize);
    }

    virtual void TearDown() {
        delete output;
        mock.reset();
    }

    void executeWhenDataReceived(const std::string& data) {
        {
            std::unique_lock<std::mutex> lock(logMutex);
            const std::string newData (data);

            unsigned int id, seq;
            char message[strlen(logMessageSent) + 1];
            std::istringstream ss{newData};

            while (ss.peek() != EOF) {
                char c;
                do {
                    c = ss.get();
                } while ((c != ']') && (c != EOF));

                ss >> id >> seq;
                ss.seekg(1, std::ios_base::cur);
                ss.read(message, strlen(logMessageSent));
                message[strlen(logMessageSent)] = '\0';

                ASSERT_TRUE(id <= logTotalThreads);
                ASSERT_TRUE(logData[id]++ == seq);
                ASSERT_TRUE(strlen(message) == strlen(logMessageSent));
                ASSERT_TRUE(!strcmp(logMessageSent, message));
            }

            for (auto value : logData) {
                if (value != logTotalSize) {
                    return;
                }
            }
        }
        {
            std::unique_lock<std::mutex> lock(mutex);
            isReady = true;
            condition.notify_one();
        }
    }

    void waitUntilTestFinished() {
        std::unique_lock<std::mutex> lock(mutex);
        auto time = std::chrono::steady_clock::now() + 500ms;
        while (!isReady) {
            auto status = condition.wait_until(lock, time);
            if (status == std::cv_status::timeout) {
                std::cerr << "Timeout Error" << std::endl;
                ASSERT_TRUE(false);
                break;
            }
        }
    }

    // Initial configuration of TcpOutput.
    const int bufferSize = 128;
    std::chrono::milliseconds waitTime {50ms};

    // Variables used for log information verification.
    std::mutex logMutex;
    unsigned int logTotalSize {0};
    unsigned int logTotalThreads {0};
    std::vector<unsigned int> logData;
    const char* logMessage = "log data message";
    const char* logMessageSent = "log data message\n";

    // Main classes.
    Library::Logging::TcpOutput* tcpOutput;
    Library::Logging::GenericOutput* output;
    std::shared_ptr<Library::Network::GenericSocket> mock;

    // Variables used to wait until the test process all the information.
    bool isReady {false};
    std::mutex mutex;
    std::condition_variable condition;
};


TEST_F(TcpOutputTest, ReceiveSingleThreadUnitData) {
    // Initial variables configuration.
    logTotalSize = 10;
    logTotalThreads = 0;

    // Test calling TcpOutputTest::executeWhenDataReceived
    EXPECT_CALL(dynamic_cast<BlazingTest::Library::Network::GenericSocketMock&>(*mock), write(SafeMatcherCast<const std::string&>(_)))
                .WillRepeatedly(WithArg<0>(Invoke(this, &TcpOutputTest::executeWhenDataReceived)));

    // Start internal thread in TcpOutput.
    tcpOutput->start();

    // Register logData Thread.
    // Generate and send log information.
    logData.push_back(0);
    Generator generator(0, logTotalSize, std::string(logMessage));
    generator(output);

    // Wait until all log information are sent to tcp socket.
    waitUntilTestFinished();
}


TEST_F(TcpOutputTest, ReceiveSingleThreadGroupData) {
    // Initial variables configuration.
    logTotalSize = 10;
    logTotalThreads = 0;

    // Test calling TcpOutputTest::executeWhenDataReceived
    EXPECT_CALL(dynamic_cast<BlazingTest::Library::Network::GenericSocketMock&>(*mock), write(SafeMatcherCast<const std::string&>(_)))
                .WillRepeatedly(WithArg<0>(Invoke(this, &TcpOutputTest::executeWhenDataReceived)));

    // Start internal thread in TcpOutput.
    tcpOutput->start();

    // Register logData Thread.
    // Generate and send log information.
    logData.push_back(0);
    Generator generator(0, logTotalSize, std::string(logMessage));
    generator(output);

    // Wait until all log information are sent to tcp socket.
    waitUntilTestFinished();
}


TEST_F(TcpOutputTest, ReceiveMultipleThreadUnitData) {
    // Initial variables configuration.
    logTotalSize = 10;
    logTotalThreads = 10;

    // Test calling TcpOutputTest::executeWhenDataReceived
    EXPECT_CALL(dynamic_cast<BlazingTest::Library::Network::GenericSocketMock&>(*mock), write(SafeMatcherCast<const std::string&>(_)))
                .WillRepeatedly(WithArg<0>(Invoke(this, &TcpOutputTest::executeWhenDataReceived)));

    // Start internal thread in TcpOutput.
    tcpOutput->start();

    // Register logData Thread.
    // Generate and send log information.
    for (unsigned int k = 0; k < logTotalThreads; ++k) {
        logData.push_back(0);
    }

    std::vector<std::thread> threads;
    for (unsigned int k = 0; k < logTotalThreads; ++k) {
        Generator generator(k, logTotalSize, std::string(logMessage));
        threads.push_back(std::thread{std::move(generator), output});
    }

    // Wait for threads
    for (unsigned int k = 0; k < logTotalThreads; ++k) {
        threads[k].join();
    }

    // Wait until all log information are sent to tcp socket.
    waitUntilTestFinished();
}


TEST_F(TcpOutputTest, ReceiveMultipleThreadGroupData) {
    // Initial variables configuration.
    logTotalSize = 10;
    logTotalThreads = 10;

    // Test calling TcpOutputTest::executeWhenDataReceived
    EXPECT_CALL(dynamic_cast<BlazingTest::Library::Network::GenericSocketMock&>(*mock), write(SafeMatcherCast<const std::string&>(_)))
                .WillRepeatedly(WithArg<0>(Invoke(this, &TcpOutputTest::executeWhenDataReceived)));

    // Start internal thread in TcpOutput.
    tcpOutput->start();

    // Register logData Thread.
    // Generate and send log information.
    for (unsigned int k = 0; k < logTotalThreads; ++k) {
        logData.push_back(0);
    }

    std::vector<std::thread> threads;
    for (unsigned int k = 0; k < logTotalThreads; ++k) {
        Generator generator(k, logTotalSize, std::string(logMessage));
        threads.push_back(std::thread{std::move(generator), output});
    }

    // Wait for threads
    for (unsigned int k = 0; k < logTotalThreads; ++k) {
        threads[k].join();
    }

    // Wait until all log information are sent to tcp socket.
    waitUntilTestFinished();
}
