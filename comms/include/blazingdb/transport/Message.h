#pragma once

#include <cassert>
#include <memory>
#include <vector>
#include "ColumnTransport.h"
#include "blazingdb/transport/Node.h"
#include "blazingdb/transport/common/macros.hpp"
#include <rmm/device_buffer.hpp>

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
                                std::vector<ColumnTransport>, std::vector<std::unique_ptr<rmm::device_buffer>> >;

public:
  explicit GPUMessage(std::string const &messageToken,
                      uint32_t const &contextToken,
                      const blazingdb::transport::experimental::Node &sender_node)
      : Message{messageToken, contextToken, sender_node} {}

  virtual raw_buffer GetRawColumns() {
    assert(true);
  }

  BZ_INTERFACE(GPUMessage);
};

class GPUReceivedMessage : public Message  {
public:
  explicit GPUReceivedMessage(std::string const &messageToken,
                      uint32_t const &contextToken,
                      const blazingdb::transport::experimental::Node &sender_node)
      : Message{messageToken, contextToken, sender_node} {}

  BZ_INTERFACE(GPUReceivedMessage);
};

} // namespace experimental 

}  // namespace transport
}  // namespace blazingdb
