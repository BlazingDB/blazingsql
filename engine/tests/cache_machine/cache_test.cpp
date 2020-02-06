#include <gtest/gtest.h>
#include <iostream>
#include <rmm/rmm.h>

#include <execution_graph/logic_controllers/LogicPrimitives.h>

struct CacheMachineTest : public ::testing::Test {
  CacheMachineTest() {

  }

  ~CacheMachineTest() {
  }

  void SetUp() override {
      rmmInitialize(nullptr);
  }

  void TearDown() override {
      rmmFinalize();
  }
};
