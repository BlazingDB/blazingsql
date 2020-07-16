#include <iostream>
#include <chrono>
#include <thread>
#include <gtest/gtest.h>

#include "execution_graph/logic_controllers/CacheMachine.h"  // WaitingQueue
#include "execution_graph/logic_controllers/LogicPrimitives.h"  // BlazingTable
#include "execution_graph/logic_controllers/BlazingColumn.h"  // BlazingColumn
#include "execution_graph/logic_controllers/BlazingColumnOwner.h"  // BlazingColumnOwner

#define DESCR(d) RecordProperty("description", d)

using namespace ral;


class WaitingQueueTest : public ::testing::Test {
protected:
   void SetUp() override {
   }

   // void TearDown() override {}

   // Minimum required to create and return a cache::message instance for use
   // with a WaitingQueue.
   std::unique_ptr<cache::message>
   createCacheMsg(std::string msgId) {
      std::vector<std::unique_ptr<frame::BlazingColumn>> blazingColumns;
      std::vector<std::string> colNames = {};

      auto blazingTable = std::make_unique<frame::BlazingTable>(std::move(blazingColumns), colNames);
      auto content = std::make_unique<cache::GPUCacheData>(std::move(blazingTable));

      return std::move(std::make_unique<cache::message>(std::move(content), msgId));
   }

   // Calls put() on a WaitingQueue instance pointer after waiting delayMs
   std::thread
   putCacheMsgAfter(int delayMs, cache::WaitingQueue* wqPtr, std::unique_ptr<cache::message> msg) {
      return std::thread(&WaitingQueueTest::putCacheMsgAfterWorker, this, delayMs, wqPtr, std::move(msg));
   }

private:
   void
   putCacheMsgAfterWorker(int delayMs, cache::WaitingQueue* wqPtr, std::unique_ptr<cache::message> msg) {
      std::this_thread::sleep_for(std::chrono::milliseconds(delayMs));
      wqPtr->put(std::move(msg));
   }
};


TEST_F(WaitingQueueTest, putPop) {
   DESCR("simple test of single put-pop");

   cache::WaitingQueue wq;
   std::string msgId = "uniqueId1";

   wq.put(createCacheMsg(msgId));
   auto msgOut = wq.pop_or_wait();

   ASSERT_NE(msgOut, nullptr);
   EXPECT_EQ(msgOut->get_message_id(), msgId);
}


TEST_F(WaitingQueueTest, putPopMultiple) {
   DESCR("test of several put-pop operations ensuring proper ordering");

   cache::WaitingQueue wq;
   int numItems = 99;

   for(int i=0; i<numItems; ++i) {
      wq.put(createCacheMsg("uniqueId" + std::to_string(i)));
   }

   // Ensure msgs popped in correct order
   for(int i=0; i<numItems; ++i) {
      auto msgOut = wq.pop_or_wait();
      ASSERT_NE(msgOut, nullptr);
      EXPECT_EQ(msgOut->get_message_id(), "uniqueId" + std::to_string(i));
   }
}


TEST_F(WaitingQueueTest, putWaitForPop) {
   DESCR("ensures pop_or_wait() properly waits for a message");

   cache::WaitingQueue wq;
   std::string msgId = "uniqueId1";
   auto msg = createCacheMsg(msgId);

   // Put a message in the queue after waiting 300ms
   std::thread t = putCacheMsgAfter(300, &wq, std::move(msg));

   // This should wait until the message is present
   // FIXME: if this times out, a crash results from calling spdlog (CacheMachine.h:202)
   auto msgOut = wq.pop_or_wait();

   ASSERT_NE(msgOut, nullptr);
   EXPECT_EQ(msgOut->get_message_id(), msgId);
   t.join();  // should not be needed
}


TEST_F(WaitingQueueTest, putGet) {
   DESCR("simple test of single put-get");

   cache::WaitingQueue wq;
   std::string msgId = "uniqueId1";

   wq.put(createCacheMsg(msgId));
   auto msgOut = wq.get_or_wait(msgId);

   ASSERT_NE(msgOut, nullptr);
   EXPECT_EQ(msgOut->get_message_id(), msgId);

}


TEST_F(WaitingQueueTest, putGetWaitForId) {
   DESCR("ensures get waits for msg with proper ID");

   cache::WaitingQueue wq;
   std::string msgId1 = "uniqueId1";
   std::string msgId2 = "uniqueId2";

   // Put mgs1 in the queue immediately and msg2 after 300ms
   wq.put(createCacheMsg(msgId1));
   std::thread t = putCacheMsgAfter(300, &wq, createCacheMsg(msgId2));

   // msg with ID 2 won't show up for ~300ms, so this should wait
   // FIXME: this will wait forever or crash with a spdlog problem (CacheMachine.h:268)
   auto msgOut = wq.get_or_wait(msgId2);
   ASSERT_NE(msgOut, nullptr);
   EXPECT_EQ(msgOut->get_message_id(), msgId2);

   // msg with ID 1 should still be present
   msgOut = wq.get_or_wait(msgId1);
   ASSERT_NE(msgOut, nullptr);
   EXPECT_EQ(msgOut->get_message_id(), msgId1);
   t.join();  // should not be needed
}


/*
TODO:
  * test lock()
*/
