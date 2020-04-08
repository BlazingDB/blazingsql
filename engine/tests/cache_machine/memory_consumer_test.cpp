#include <thread>
#include <src/from_cudf/cpp_tests/utilities/base_fixture.hpp>

#include "tests/utilities/MemoryConsumer.cuh"

struct MemoryConsumerTest : public cudf::test::BaseFixture {
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
