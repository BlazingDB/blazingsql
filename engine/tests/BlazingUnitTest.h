#include <cstdlib>
#include <iostream>
#include <string>
#include <vector>

#include <spdlog/spdlog.h>
#include <spdlog/async.h>
#include <spdlog/sinks/stdout_color_sinks.h>

#include <gtest/gtest.h>

#include <CalciteExpressionParsing.h>
#include <CalciteInterpreter.h>
#include <bmr/initializer.h>


struct BlazingUnitTest : public ::testing::Test {
	static void SetUpTestSuite() {
		spdlog::init_thread_pool(8192, 1);
		auto stdout_sink = std::make_shared<spdlog::sinks::stdout_color_sink_mt>();
		spdlog::sinks_init_list sink_list = { stdout_sink };
		auto logger = std::make_shared<spdlog::async_logger>("batch_logger", sink_list, spdlog::thread_pool(), spdlog::async_overflow_policy::block);
		logger->set_level(spdlog::level::off);
		spdlog::register_logger(logger);
	}

	static void TearDownTestSuite() {
		spdlog::shutdown();
	}

	void SetUp() {
		rmmInitialize(nullptr);
		BlazingRMMInitialize(nullptr);
	}

	void TearDown() { 
		ASSERT_EQ(rmmFinalize(), RMM_SUCCESS); 
		BlazingRMMFinalize();
	}
};
