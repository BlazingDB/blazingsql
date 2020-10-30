#pragma once

#include <bmr/initializer.h>
#include "cudf_test/cudf_gtest.hpp"


struct BlazingUnitTest : public ::testing::Test {
	static void SetUpTestSuite();
	static void TearDownTestSuite();

	virtual void SetUp() override {
		BlazingRMMInitialize("cuda_memory_resource");
	}

	virtual void TearDown() override {
		BlazingRMMFinalize();
	}
};
