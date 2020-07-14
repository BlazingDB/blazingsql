#pragma once

#include <bmr/initializer.h>
#include <gtest/gtest.h>


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
