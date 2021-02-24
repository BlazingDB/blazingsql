#include <BlazingUnitTest.h>

#include <spdlog/spdlog.h>
#include <spdlog/async.h>
#include <spdlog/sinks/stdout_color_sinks.h>



void BlazingUnitTest::SetUpTestSuite() {
	spdlog::init_thread_pool(8192, 1);

}

void BlazingUnitTest::TearDownTestSuite() {
	spdlog::shutdown();
}
