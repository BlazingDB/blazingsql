#include <gtest/gtest.h>
#include <vector>
#include <thread>
#include <sstream>
#include <iostream>
#include "Library/Logging/CoutOutput.h"

struct CoutOutputTest : public ::testing::Test {
    CoutOutputTest() {
    }

    virtual ~CoutOutputTest() {
    }

    virtual void SetUp() {
        output = new Library::Logging::CoutOutput();

        cout_buffer = std::cout.rdbuf();
        std::cout.rdbuf(buffer.rdbuf());
    }

    virtual void TearDown() {
        delete output;

        std::cout.rdbuf(cout_buffer);
    }

    void verifyBuffer(int times, std::stringstream& buffer, const std::string& outLogData) {
        char* var = new char[outLogData.length() + 1];

        int counter = 0;
        while (buffer.peek() != EOF) {
            counter++;
            var[0] = '\0';

            buffer.read(var, outLogData.length());
            var[outLogData.length()] = '\0';

            ASSERT_TRUE(!strcmp(var, outLogData.c_str()));
        }

        ASSERT_TRUE(counter == times);
        delete[] var;
    }

    std::stringstream buffer;
    std::streambuf *cout_buffer;

    const std::string logData {"sample data"};
    const std::string outLogData = logData + '\n';

    Library::Logging::GenericOutput* output;
};


TEST_F(CoutOutputTest, EvaluateSingleThreadSinglePrint) {
    const int times = 1;

    for (int k = 0; k < times; ++k) {
        output->flush(logData);
    }

    verifyBuffer(times, buffer, outLogData);
}


TEST_F(CoutOutputTest, EvaluateSingleThreadMultiPrint) {
    const int times = 10;

    for (int k = 0; k < times; ++k) {
        output->flush(logData);
    }

    verifyBuffer(times, buffer, outLogData);
}


TEST_F(CoutOutputTest, EvaluateMultiThreadSinglePrint) {
    const int times = 1;
    const int threadSize = 10;

    std::vector<std::thread> threads;
    for (int k = 0; k < threadSize; ++k) {
        threads.push_back(std::thread([this]() {
            output->flush(logData);
        }));
    }

    for (int k = 0; k < threadSize; ++k) {
        threads[k].join();
    }

    verifyBuffer(times * threadSize, buffer, outLogData);
}


TEST_F(CoutOutputTest, EvaluateMultiThreadMultiPrint) {
    const int times = 10;
    const int threadSize = 10;

    std::vector<std::thread> threads;
    for (int k = 0; k < threadSize; ++k) {
        threads.push_back(std::thread([this, times]() {
            for (int i = 0; i < times; ++i) {
                output->flush(logData);
            }
        }));
    }

    for (int k = 0; k < threadSize; ++k) {
        threads[k].join();
    }

    verifyBuffer(times * threadSize, buffer, outLogData);
}
