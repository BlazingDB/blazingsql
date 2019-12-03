#include <chrono>
#include <cstdlib>
#include <iostream>
#include <stdlib.h>
#include <unistd.h>
#include <vector>

#include "gtest/gtest.h"

#include "ExceptionHandling/BlazingException.h"
#include "ExceptionHandling/BlazingThread.h"

void noArgFunc() { std::cout << "noArgFunc" << std::endl; }
void oneArgValFunc(std::string arg) {
	std::string message = "oneArgValFunc" + arg;
	std::cout << message << std::endl;
}
void oneArgRefFunc(std::string & arg) {
	std::string message = "oneArgRefFunc" + arg;
	std::cout << message << std::endl;
	arg = message;
}
void oneArgConstValFunc(const std::string arg) {
	std::string message = "oneArgConstValFunc" + arg;
	std::cout << message << std::endl;
}
void oneArgConstRefFunc(const std::string & arg) {
	std::string message = "oneArgConstRefFunc" + arg;
	std::cout << message << std::endl;
}
void manyArgFunc(std::string arg, int num, std::string & refArg) {
	std::string message = "manyArgFunc" + std::to_string(num) + arg;
	std::cout << message << std::endl;
	refArg = message;
}


class TestClass {
public:
	TestClass(int newX) {
		this->x = newX;
		this->messages.resize(6, "");
		this->threads.resize(6);
	}

	std::vector<std::string> getMessages() { return messages; }

	void noArgFunc() { messages[0] += "noArgFunc" + std::to_string(this->x); }
	void oneArgValFunc(std::string arg) { messages[1] += "oneArgValFunc" + arg; }
	void oneArgRefFunc(std::string & arg) {
		messages[2] += "oneArgRefFunc" + arg;
		arg = messages[2];
	}
	void oneArgConstValFunc(const std::string arg) { messages[3] += "oneArgConstValFunc" + arg; }
	void oneArgConstRefFunc(const std::string & arg) { messages[4] += "oneArgConstRefFunc" + arg; }
	void manyArgFunc(std::string arg, int num, std::string & refArg) {
		messages[5] += "manyArgFunc" + std::to_string(num) + arg + std::to_string(x);
		refArg = messages[5];
	}

	void createSubThread0() { threads[0] = BlazingThread(&TestClass::noArgFunc, this); }
	void createSubThread1(std::string msg) { threads[1] = BlazingThread(&TestClass::oneArgValFunc, this, msg); }
	void createSubThread2(std::string & msg) {
		threads[2] = BlazingThread(&TestClass::oneArgRefFunc, this, std::ref(msg));
	}
	void createSubThread3(std::string msg) { threads[3] = BlazingThread(&TestClass::oneArgConstValFunc, this, msg); }
	void createSubThread4(std::string & msg) {
		threads[4] = BlazingThread(&TestClass::oneArgConstRefFunc, this, std::ref(msg));
	}
	void createSubThread5(std::string msg, std::string & refArg) {
		threads[5] = BlazingThread(&TestClass::manyArgFunc, this, msg, 123, std::ref(refArg));
	}


	void functionWithErrorNoArgs() { throw BlazingException("A planned error NoArgs!"); }
	void functionWithErrorWithArgs(int x, int y, int & z) {
		z = x + y;
		std::string message = "A planned error WithArgs z=" + std::to_string(z);
		throw BlazingException(message);
	}
	void functionThatCallsWithErrorWithArgs(int x, int y, int & z) {
		short numThreads = 3;
		std::vector<BlazingThread> threads(numThreads);

		for(int i = 0; i < threads.size(); i++) {
			int xi = x + i;
			threads[i] = BlazingThread([this, xi, y, &z] { TestClass::functionWithErrorWithArgs(xi, y, z); });
		}
		for(int i = 0; i < threads.size(); i++) {
			threads[i].join();
		}
	}

	void functionThatCallsNoArgs() {
		std::cout << "functionThatCallsNoArgs" << std::endl;
		functionWithErrorNoArgs();
	}
	void functionThatCallsWithArgs(int x, int y, int & z) {
		std::cout << "functionThatCallsNoArgs" << std::endl;
		x++;
		y++;
		functionWithErrorWithArgs(x, y, z);
	}

	void functionThatRethrowsNoArgs() {
		try {
			this->functionWithErrorNoArgs();
		} catch(BlazingException & e) {
			e.append("functionThatRethrowsNoArgs");
			throw;
		}
	}
	void functionThatRethrowsWithArgs(int x, int y, int & z) {
		try {
			this->functionWithErrorWithArgs(x, y, z);
		} catch(BlazingException & e) {
			e.append("functionWithErrorWithArgs");
			throw;
		}
	}


	void joinAll() {
		for(int i = 0; i < threads.size(); i++) {
			threads[i].join();
		}
	}

	std::vector<BlazingThread> threads;


private:
	int x;
	std::vector<std::string> messages;
};


TEST(BlazingThreadTest, BlazingThreadNoErrors) {
	{
		try {
			short reps = 10;
			std::vector<TestClass *> testClasses(reps);
			std::vector<std::vector<std::string>> messages(reps);
			std::vector<std::vector<std::string>> expecteds(reps);
			for(int i = 0; i < reps; i++) {
				TestClass * test = new TestClass(i);
				testClasses[i] = test;
			}

			for(int i = 0; i < reps; i++) {
				messages[i].resize(6);
				expecteds[i].resize(6);

				testClasses[i]->createSubThread0();
				expecteds[i][0] = "noArgFunc" + std::to_string(i);

				messages[i][1] = "createSubThread1" + std::to_string(i);
				testClasses[i]->createSubThread1(messages[i][1]);
				expecteds[i][1] = "oneArgValFunccreateSubThread1" + std::to_string(i);

				messages[i][2] = "createSubThread2" + std::to_string(i);
				testClasses[i]->createSubThread2(messages[i][2]);
				expecteds[i][2] = "oneArgRefFunccreateSubThread2" + std::to_string(i);

				messages[i][3] = "createSubThread3" + std::to_string(i);
				testClasses[i]->createSubThread3(messages[i][3]);
				expecteds[i][3] = "oneArgConstValFunccreateSubThread3" + std::to_string(i);

				messages[i][4] = "createSubThread4" + std::to_string(i);
				testClasses[i]->createSubThread4(messages[i][4]);
				expecteds[i][4] = "oneArgConstRefFunccreateSubThread4" + std::to_string(i);

				messages[i][5] = "init";
				testClasses[i]->createSubThread5("createSubThread5", messages[i][5]);
				expecteds[i][5] = "manyArgFunc123createSubThread5" + std::to_string(i);
			}
			for(int i = 0; i < reps; i++) {
				testClasses[i]->joinAll();
			}
			for(int i = 0; i < reps; i++) {
				std::vector<std::string> out = testClasses[i]->getMessages();

				for(int j = 0; j < expecteds[i].size(); j++) {
					EXPECT_EQ(out[j], expecteds[i][j]);
				}
				EXPECT_EQ(messages[i][5], expecteds[i][5]);
			}
			for(int i = 0; i < reps; i++) {
				delete testClasses[i];
			}

		} catch(const BlazingException & e) {
			EXPECT_TRUE(false);  // should not be catching an error here
		}
	}
	{
		try {
			short reps = 10;
			std::vector<std::vector<std::string>> messages(reps);
			std::vector<std::vector<std::string>> expecteds(reps);
			std::vector<std::vector<BlazingThread>> threads(reps);

			for(int i = 0; i < reps; i++) {
				threads[i].resize(6);
				messages[i].resize(6);
				expecteds[i].resize(6);

				threads[i][0] = BlazingThread(&noArgFunc);

				messages[i][1] = "oneArgValFunc" + std::to_string(i);
				threads[i][1] = BlazingThread(&oneArgValFunc, messages[i][1]);

				messages[i][2] = "oneArgRefFunc" + std::to_string(i);
				threads[i][2] = BlazingThread(&oneArgRefFunc, std::ref(messages[i][2]));
				expecteds[i][2] = "oneArgRefFunconeArgRefFunc" + std::to_string(i);

				messages[i][3] = "oneArgConstValFunc" + std::to_string(i);
				threads[i][3] = BlazingThread(&oneArgConstValFunc, messages[i][3]);

				messages[i][4] = "oneArgConstRefFunc" + std::to_string(i);
				threads[i][4] = BlazingThread(&oneArgConstRefFunc, std::ref(messages[i][4]));

				std::string message = "manyArgFunc" + std::to_string(i);
				threads[i][5] = BlazingThread(&manyArgFunc, message, 123, std::ref(messages[i][5]));
				expecteds[i][5] = "manyArgFunc" + std::to_string(123) + message;
			}
			for(int i = 0; i < reps; i++) {
				for(int j = 0; j < threads[i].size(); j++) {
					threads[i][j].join();
				}
			}
			for(int i = 0; i < reps; i++) {
				EXPECT_EQ(messages[i][2], expecteds[i][2]);
				EXPECT_EQ(messages[i][5], expecteds[i][5]);
			}

		} catch(const BlazingException & e) {
			EXPECT_TRUE(false);  // should not be catching an error here
		}
	}
}


TEST(BlazingThreadTest, BlazingExceptionCaughtErrors) {
	{
		try {
			TestClass test = TestClass(123);
			test.functionWithErrorNoArgs();

			EXPECT_TRUE(false);  // should not get here because we should be catching an error
		} catch(const BlazingException & e) {
			std::string expected = "A planned error NoArgs!";
			EXPECT_EQ(e.what(), expected);
		}
	}
	{
		try {
			TestClass test = TestClass(123);
			int z = 0;
			test.functionWithErrorWithArgs(1, 2, z);

			EXPECT_TRUE(false);  // should not get here because we should be catching an error
		} catch(const BlazingException & e) {
			std::string expected = "A planned error WithArgs z=" + std::to_string(3);
			EXPECT_EQ(e.what(), expected);
		}
	}
	{
		try {
			TestClass test = TestClass(123);
			test.functionThatCallsNoArgs();

			EXPECT_TRUE(false);  // should not get here because we should be catching an error
		} catch(const BlazingException & e) {
			std::string expected = "A planned error NoArgs!";
			EXPECT_EQ(e.what(), expected);
		}
	}
	{
		try {
			TestClass test = TestClass(123);
			int z = 0;
			test.functionThatCallsWithArgs(1, 2, z);

			EXPECT_TRUE(false);  // should not get here because we should be catching an error
		} catch(const BlazingException & e) {
			std::string expected = "A planned error WithArgs z=" + std::to_string(5);
			EXPECT_EQ(e.what(), expected);
		}
	}
	{
		try {
			TestClass test = TestClass(123);
			test.functionThatRethrowsNoArgs();

			EXPECT_TRUE(false);  // should not get here because we should be catching an error
		} catch(const BlazingException & e) {
			std::string expected = "functionThatRethrowsNoArgs\nA planned error NoArgs!";
			EXPECT_EQ(e.what(), expected);
		}
	}
	{
		try {
			TestClass test = TestClass(123);
			int z = 0;
			test.functionThatRethrowsWithArgs(1, 2, z);

			EXPECT_TRUE(false);  // should not get here because we should be catching an error
		} catch(const BlazingException & e) {
			std::string expected = "functionWithErrorWithArgs\nA planned error WithArgs z=" + std::to_string(3);
			EXPECT_EQ(e.what(), expected);
		}
	}
}


TEST(BlazingThreadTest, BlazingThreadWithCaughtErrors) {
	{
		try {
			std::cout << "test 1" << std::endl;
			TestClass test = TestClass(123);
			BlazingThread thread = BlazingThread(&TestClass::functionWithErrorNoArgs, &test);
			thread.join();

			EXPECT_TRUE(false);  // should not get here because we should be catching an error
		} catch(const BlazingException & e) {
			std::string expected = "A planned error NoArgs!";
			EXPECT_EQ(e.what(), expected);
		}
	}
	{
		try {
			std::cout << "test 2" << std::endl;
			TestClass test = TestClass(123);
			int z = 0;
			BlazingThread thread = BlazingThread(&TestClass::functionWithErrorWithArgs, &test, 1, 2, std::ref(z));
			thread.join();

			EXPECT_TRUE(false);  // should not get here because we should be catching an error
		} catch(const BlazingException & e) {
			std::string expected = "A planned error WithArgs z=" + std::to_string(3);
			EXPECT_EQ(e.what(), expected);
		} catch(const std::exception & e) {
			std::string expected = "A planned error WithArgs z=" + std::to_string(3);
			EXPECT_EQ(e.what(), expected);
		}
	}
	{
		try {
			std::cout << "test 3" << std::endl;
			TestClass test = TestClass(123);
			int z = 0;
			BlazingThread thread;
			thread = BlazingThread(&TestClass::functionWithErrorWithArgs, &test, 1, 2, std::ref(z));
			thread.join();

			EXPECT_TRUE(false);  // should not get here because we should be catching an error
		} catch(const BlazingException & e) {
			std::string expected = "A planned error WithArgs z=" + std::to_string(3);
			EXPECT_EQ(e.what(), expected);
		} catch(const std::exception & e) {
			std::string expected = "A planned error WithArgs z=" + std::to_string(3);
			EXPECT_EQ(e.what(), expected);
		}
	}
	{
		try {
			std::cout << "test 4" << std::endl;
			TestClass test = TestClass(123);
			int z = 0;
			short numThreads = 10;
			std::vector<BlazingThread> threads(numThreads);

			for(int i = 0; i < threads.size(); i++) {
				int xi = 1 + i;
				threads[i] = BlazingThread(&TestClass::functionWithErrorWithArgs, &test, xi, 2, std::ref(z));
			}
			for(int i = 0; i < threads.size(); i++) {
				threads[i].join();
			}

			EXPECT_TRUE(false);  // should not get here because we should be catching an error
		} catch(const BlazingException & e) {
			std::string eStr(e.what());
			std::cout << eStr << std::endl;
			std::string expected0 = "A planned error WithArgs z=" + std::to_string(3);
			std::string expected1 = "A planned error WithArgs z=" + std::to_string(4);
			std::string expected2 = "A planned error WithArgs z=" + std::to_string(5);
			EXPECT_TRUE(eStr == expected0 || eStr == expected1 || eStr == expected2);
		}
	}
	{
		try {
			std::cout << "test 5" << std::endl;
			TestClass test = TestClass(123);
			int z = 0;
			short numThreads = 3;
			std::vector<BlazingThread> threads(numThreads);

			for(int i = 0; i < threads.size(); i++) {
				int xi = 1 + i;
				int y = 2;
				threads[i] = BlazingThread([&test, xi, y, &z] { test.functionWithErrorWithArgs(xi, y, z); });
			}
			for(int i = 0; i < threads.size(); i++) {
				threads[i].join();
			}

			EXPECT_TRUE(false);  // should not get here because we should be catching an error
		} catch(const BlazingException & e) {
			std::string eStr(e.what());
			std::cout << eStr << std::endl;
			std::string expected0 = "A planned error WithArgs z=" + std::to_string(3);
			std::string expected1 = "A planned error WithArgs z=" + std::to_string(4);
			std::string expected2 = "A planned error WithArgs z=" + std::to_string(5);
			EXPECT_TRUE(eStr == expected0 || eStr == expected1 || eStr == expected2);
		}
	}
	{
		try {
			std::cout << "test 6" << std::endl;
			TestClass test = TestClass(123);
			int z = 0;
			test.functionThatCallsWithErrorWithArgs(1, 2, z);

			EXPECT_TRUE(false);  // should not get here because we should be catching an error

		} catch(const BlazingException & e) {
			std::string eStr(e.what());
			std::cout << eStr << std::endl;
			std::string expected0 = "A planned error WithArgs z=" + std::to_string(3);
			std::string expected1 = "A planned error WithArgs z=" + std::to_string(4);
			std::string expected2 = "A planned error WithArgs z=" + std::to_string(5);
			EXPECT_TRUE(eStr == expected0 || eStr == expected1 || eStr == expected2);
		}
	}
	{
		try {
			std::cout << "test 7" << std::endl;
			TestClass test = TestClass(123);
			int z = 0;
			BlazingThread thread =
				BlazingThread(&TestClass::functionThatCallsWithErrorWithArgs, &test, 1, 2, std::ref(z));
			thread.join();

			EXPECT_TRUE(false);  // should not get here because we should be catching an error

		} catch(const BlazingException & e) {
			std::string eStr(e.what());
			std::cout << eStr << std::endl;
			std::string expected0 = "A planned error WithArgs z=" + std::to_string(3);
			std::string expected1 = "A planned error WithArgs z=" + std::to_string(4);
			std::string expected2 = "A planned error WithArgs z=" + std::to_string(5);
			EXPECT_TRUE(eStr == expected0 || eStr == expected1 || eStr == expected2);
		}
	}
	{
		try {
			std::cout << "test 8" << std::endl;
			TestClass test = TestClass(123);
			BlazingThread thread = BlazingThread(&TestClass::functionThatCallsNoArgs, &test);
			thread.join();

			EXPECT_TRUE(false);  // should not get here because we should be catching an error
		} catch(const BlazingException & e) {
			std::string expected = "A planned error NoArgs!";
			EXPECT_EQ(e.what(), expected);
		}
	}
	{
		try {
			std::cout << "test 9" << std::endl;
			TestClass test = TestClass(123);
			int z = 0;
			BlazingThread thread = BlazingThread(&TestClass::functionThatCallsWithArgs, &test, 1, 2, std::ref(z));
			thread.join();

			EXPECT_TRUE(false);  // should not get here because we should be catching an error
		} catch(const BlazingException & e) {
			std::string expected = "A planned error WithArgs z=" + std::to_string(5);
			EXPECT_EQ(e.what(), expected);
		}
	}
	{
		try {
			TestClass test = TestClass(123);
			BlazingThread thread = BlazingThread(&TestClass::functionThatRethrowsNoArgs, &test);
			thread.join();

			EXPECT_TRUE(false);  // should not get here because we should be catching an error
		} catch(const BlazingException & e) {
			std::string expected = "functionThatRethrowsNoArgs\nA planned error NoArgs!";
			EXPECT_EQ(e.what(), expected);
		}
	}
	{
		try {
			TestClass test = TestClass(123);
			int z = 0;
			BlazingThread thread = BlazingThread(&TestClass::functionThatRethrowsWithArgs, &test, 1, 2, std::ref(z));
			thread.join();

			EXPECT_TRUE(false);  // should not get here because we should be catching an error
		} catch(const BlazingException & e) {
			std::string expected = "functionWithErrorWithArgs\nA planned error WithArgs z=" + std::to_string(3);
			EXPECT_EQ(e.what(), expected);
		}
	}
}
