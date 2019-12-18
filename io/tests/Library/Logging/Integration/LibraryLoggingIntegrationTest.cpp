#include "GenericSocketMock.h"
#include "Library/Logging/CoutOutput.h"
#include "Library/Logging/Logger.h"
#include "Library/Logging/ServiceLogging.h"
#include "Library/Logging/TcpOutput.h"
#include <chrono>
#include <ctime>
#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <mutex>
#include <ostream>
#include <thread>
#include <vector>

using testing::_;
using testing::Invoke;
using testing::SafeMatcherCast;
using testing::WithArg;

using namespace std::chrono_literals;

struct LibraryLoggingIntegrationTest : public ::testing::Test {
	LibraryLoggingIntegrationTest() {}

	virtual ~LibraryLoggingIntegrationTest() {}

	virtual void SetUp() {}

	virtual void TearDown() {}

	template <typename Function>
	void executeFunction(
		const int threadSize, const int messageTimes, const std::string & messageData, Function && function) {
		std::vector<std::thread> threads;
		threads.reserve(threadSize);

		for(int k = 0; k < threadSize; ++k) {
			threads.push_back(std::thread(function, messageTimes, messageData));
		}

		for(auto & thread : threads) {
			thread.join();
		}
	}

	void verifyForCoutOutput(std::stringstream & buffer) {
		char * message = new char[messagePrinted.length() + 1];

		while(buffer.peek() != EOF) {
			buffer.read(message, messagePrinted.length());
			message[messagePrinted.length()] = '\0';

			ASSERT_TRUE(strlen(message) == messagePrinted.length());
			ASSERT_TRUE(!strcmp(message, messagePrinted.c_str()));
		}

		delete[] message;
	}

	void verifyForTcpOutput(const std::string & data) {
		{
			std::unique_lock<std::mutex> lock(logMutex);

			char * message = new char[messagePrinted.length() + 1];
			std::istringstream ss{data};

			while(ss.peek() != EOF) {
				char c;
				do {
					c = ss.get();
				} while((c != ']') && (c != EOF));

				unsigned int readData = ss.readsome(message, messagePrinted.length());
				message[messagePrinted.length()] = '\0';

				counter++;
				ASSERT_TRUE(readData == messagePrinted.length());
				ASSERT_TRUE(!strcmp(message, messagePrinted.c_str()));
			}

			delete[] message;
			if(counter < (threadSize * messageTimes)) {
				return;
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
		auto time = std::chrono::steady_clock::now() + 250ms;
		while(!isReady) {
			auto status = condition.wait_until(lock, time);
			if(status == std::cv_status::timeout) {
				std::cerr << "Timeout Error" << std::endl;
				ASSERT_TRUE(false);
				break;
			}
		}
	}

	// control variables
	std::mutex logMutex;
	int counter{0};

	// testing variables
	const int threadSize{10};
	const int messageTimes{10};
	const std::string messageData{"sample data"};
	const std::string messagePrinted = messageData + "\n";

	// variables used to wait until the test process all the information.
	bool isReady{false};
	std::mutex mutex;
	std::condition_variable condition;
};


TEST_F(LibraryLoggingIntegrationTest, IntegrationCoutOutput) {
	// change cout buffer
	std::streambuf * cout_buffer = std::cout.rdbuf();
	std::stringstream buffer;
	std::cout.rdbuf(buffer.rdbuf());

	// configuration
	Library::Logging::ServiceLogging::getInstance().setLogOutput(new Library::Logging::CoutOutput());

	// perform test
	executeFunction(
		threadSize, messageTimes, messageData, [this](const int messageTimes, const std::string & messageData) {
			for(int k = 0; k < messageTimes; ++k) {
				Library::Logging::Logger().log(messageData);
			}
		});

	// get back cout buffer
	std::cout.rdbuf(cout_buffer);

	// verify result
	verifyForCoutOutput(buffer);
}


TEST_F(LibraryLoggingIntegrationTest, IntegrationTcpOutput) {
	namespace Logging = Library::Logging;
	namespace Network = Library::Network;
	using GenericSocketMock = BlazingTest::Library::Network::GenericSocketMock;

	// configuration
	const int logWaitTimeMS = 10;
	const int logBufferSize = 128;
	std::shared_ptr<Network::GenericSocket> mock = std::make_shared<GenericSocketMock>();

	Logging::GenericOutput * output = new Logging::TcpOutput();
	Logging::TcpOutput * tcpOutput = dynamic_cast<Logging::TcpOutput *>(output);

	tcpOutput->setSocket(mock);
	tcpOutput->setMaxBufferSize(logBufferSize);
	tcpOutput->setWaitTime(std::chrono::milliseconds(logWaitTimeMS));
	tcpOutput->start();

	Logging::ServiceLogging::getInstance().setLogOutput(output);

	// verify the test
	EXPECT_CALL(dynamic_cast<BlazingTest::Library::Network::GenericSocketMock &>(*mock),
		write(SafeMatcherCast<const std::string &>(_)))
		.WillRepeatedly(WithArg<0>(Invoke(this, &LibraryLoggingIntegrationTest::verifyForTcpOutput)));

	// perform the test
	executeFunction(
		threadSize, messageTimes, messageData, [this](const int messageTimes, const std::string & messageData) {
			for(int k = 0; k < messageTimes; ++k) {
				Library::Logging::Logger().log(messageData);
			}
		});

	// wait
	waitUntilTestFinished();
}
