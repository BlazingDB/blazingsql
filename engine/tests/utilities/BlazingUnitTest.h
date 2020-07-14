#pragma once

#include <bmr/initializer.h>
#include "tests/utilities/cudf_gtest.hpp"


struct BlazingUnitTest : public ::testing::Test {
	static void SetUpTestSuite();
	static void TearDownTestSuite();

	void SetUp() {
		BlazingRMMInitialize();
	}

	void TearDown() { 
		BlazingRMMFinalize();
	}
};
