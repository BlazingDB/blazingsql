#pragma once

#include <cassert>
#include <memory>
#include <vector>
#include "ColumnTransport.h"
#include "blazingdb/transport/Node.h"
#include "blazingdb/transport/common/macros.hpp"

namespace blazingdb {
namespace transport {

/// \brief Message interface
///
/// This is the interface for the messsage. A message can be sent between RAL
/// instances and between the RAL's and the Orchestrator
class Message {
public:
  struct MetaData {
    char messageToken[128]{};  // use  uses '\0' for string ending
    uint32_t contextToken{};
    int32_t total_row_size{};  // used by SampleToNodeMasterMessage

    //    int32_t num_columns{}; // used by: writeBuffersFromGPUTCP,
    //    readBuffersIntoGPUTCP, update everywhere! int32_t num_buffers{};
  };

protected:
  explicit Message(std::string const &messageToken,
                   uint32_t const &contextToken,
                   std::shared_ptr<blazingdb::transport::Node> sender_node)
      : node_{sender_node} {
    set_metadata(messageToken, contextToken);
  }

public:
  void set_metadata(std::string const &messageToken,
                    uint32_t const &contextToken) {
    assert(messageToken.size() < 127);  // max buffer size of internal metadata
                                        // representation for messageToken
    strcpy(this->metadata_.messageToken, messageToken.c_str());
    this->metadata_.contextToken = contextToken;
  }

  const uint32_t getContextTokenValue() const { return metadata_.contextToken; }

  const std::string getMessageTokenValue() const {
    return metadata_.messageToken;
  }

  std::shared_ptr<blazingdb::transport::Node> getSenderNode() const {
    return node_;
  }

  const MetaData &metadata() const { return metadata_; }
  
  MetaData metadata() { return metadata_; }

protected:
  MetaData metadata_;
  std::shared_ptr<blazingdb::transport::Node> node_{nullptr};

  BZ_INTERFACE(Message);
};


class GPUMessage : public Message {
public:
  using raw_buffer = std::tuple<std::vector<int>, std::vector<const char *>,
                                std::vector<ColumnTransport>>;

public:
  explicit GPUMessage(std::string const &messageToken,
                      uint32_t const &contextToken,
                      std::shared_ptr<blazingdb::transport::Node> &sender_node)
      : Message{messageToken, contextToken, sender_node} {}

  explicit GPUMessage(std::string const &messageToken,
                      uint32_t const &contextToken,
                      blazingdb::transport::Node &sender_node)
      : Message{messageToken, contextToken, std::make_shared<Node>(sender_node.address())} {}

  virtual raw_buffer GetRawColumns() = 0;

  BZ_INTERFACE(GPUMessage);
};

}  // namespace transport
}  // namespace blazingdb

namespace blazingdb {
namespace transport {

namespace experimental {

class Message {
public:
  struct MetaData {
    char messageToken[128]{};  // use  uses '\0' for string ending
    uint32_t contextToken{};
    int32_t total_row_size{};  // used by SampleToNodeMasterMessage

    //    int32_t num_columns{}; // used by: writeBuffersFromGPUTCP,
    //    readBuffersIntoGPUTCP, update everywhere! int32_t num_buffers{};
  };

protected:
  explicit Message(std::string const &messageToken,
                   uint32_t const &contextToken,
                   const blazingdb::transport::experimental::Node& sender_node)
      : node_{sender_node.address()} {
    set_metadata(messageToken, contextToken);
  }

public:
  void set_metadata(std::string const &messageToken,
                    uint32_t const &contextToken) {
    assert(messageToken.size() < 127);  // max buffer size of internal metadata
                                        // representation for messageToken
    strcpy(this->metadata_.messageToken, messageToken.c_str());
    this->metadata_.contextToken = contextToken;
  }

  const uint32_t getContextTokenValue() const { return metadata_.contextToken; }

  const std::string getMessageTokenValue() const {
    return metadata_.messageToken;
  }

  blazingdb::transport::experimental::Node& getSenderNode() {
    return node_;
  }
  const MetaData &metadata() const { return metadata_; }
  MetaData &metadata() { return metadata_; }

protected:
  MetaData metadata_;
  blazingdb::transport::experimental::Node node_;

  BZ_INTERFACE(Message);
};


class GPUMessage : public Message {
public:
  using raw_buffer = std::tuple<std::vector<int>, std::vector<const char *>,
                                std::vector<ColumnTransport>>;

public:
  explicit GPUMessage(std::string const &messageToken,
                      uint32_t const &contextToken,
                      const blazingdb::transport::experimental::Node &sender_node)
      : Message{messageToken, contextToken, sender_node} {}

  virtual raw_buffer GetRawColumns() = 0;

  BZ_INTERFACE(GPUMessage);
};

class GPUReceivedMessage : public Message  {
public:
  explicit GPUReceivedMessage(std::string const &messageToken,
                      uint32_t const &contextToken,
                      const blazingdb::transport::experimental::Node &sender_node)
      : Message{messageToken, contextToken, sender_node} {}
};

} // namespace experimental 

}  // namespace transport
}  // namespace blazingdb
