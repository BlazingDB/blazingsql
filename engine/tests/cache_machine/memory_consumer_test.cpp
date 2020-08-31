#include <thread>
#include <tests/utilities/base_fixture.hpp>

#include "tests/utilities/MemoryConsumer.cuh"
#include "tests/utilities/BlazingUnitTest.h"

struct MemoryConsumerTest : public BlazingUnitTest {
	MemoryConsumerTest() {}
	~MemoryConsumerTest() {}
};

TEST_F(MemoryConsumerTest, test) {
	MemoryConsumer mc;
	mc.setOptionsPercentage({0.5, 0.2}, 2000ms);

	//Creating a thread to execute our task
	std::thread th([&mc]()
	{
		mc.run();
	});
 
	std::this_thread::sleep_for(10s);
 
	mc.stop();
 	th.join();
}
