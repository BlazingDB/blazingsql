#include <cstdlib>
#include <iostream>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include <CalciteExpressionParsing.h>
#include <CalciteInterpreter.h>
#include <bmr/initializer.h>


struct BlazingUnitTest : public ::testing::Test {
	void SetUp() {
		rmmInitialize(nullptr);
		BlazingRMMInitialize(nullptr);
	}

	void TearDown() { 
		ASSERT_EQ(rmmFinalize(), RMM_SUCCESS); 
		BlazingRMMFinalize();
	}
};
