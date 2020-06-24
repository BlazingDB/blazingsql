#include <blazingdb/transport/api.h>

#include <gtest/gtest.h>

TEST(NodeTest, NodeCreationFromCreatedBuffer) {
  using Address = blazingdb::transport::Address;
  using Node = blazingdb::transport::Node;

  const std::shared_ptr<Node> node1 =
      std::make_shared<Node>(Node(Address::TCP("1.2.3.4", 9999, 1234), ""));
  const std::shared_ptr<Node> node2 =
      std::make_shared<Node>(Node(Address::TCP("1.2.3.4", 9999, 1234), ""));

  EXPECT_TRUE(*node1 == *node2);
}
