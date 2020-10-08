#include <BlazingUnitTest.h>

#include <spdlog/spdlog.h>
#include <spdlog/async.h>
#include <spdlog/sinks/stdout_color_sinks.h>


void create_test_logger(std::string loggingName){
	auto stdout_sink = std::make_shared<spdlog::sinks::stdout_color_sink_mt>();
	stdout_sink->set_pattern("[%T.%e] [%^%l%$] %v");
	stdout_sink->set_level(spdlog::level::warn);
	
	spdlog::sinks_init_list sink_list = { stdout_sink};
	auto logger = std::make_shared<spdlog::async_logger>(loggingName, sink_list, spdlog::thread_pool(), spdlog::async_overflow_policy::block);
	logger->set_level(spdlog::level::off);
	spdlog::register_logger(logger);	
}

void BlazingUnitTest::SetUpTestSuite() {
	spdlog::init_thread_pool(8192, 1);
	
	create_test_logger("batch_logger");
	create_test_logger("queries_logger");
	create_test_logger("kernels_logger");
	create_test_logger("kernels_edges_logger");
	create_test_logger("events_logger");
	create_test_logger("cache_events_logger");

	spdlog::flush_on(spdlog::level::warn);
	spdlog::flush_every(std::chrono::seconds(1));
}

void BlazingUnitTest::TearDownTestSuite() {
	spdlog::shutdown();
}
