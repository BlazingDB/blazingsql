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

class Message {
public:
  struct MetaData {
    char messageToken[128]{};  // use  uses '\0' for string ending
    uint32_t contextToken{};
    int64_t total_row_size{};  // used by SampleToNodeMasterMessage
    int32_t n_batches{1};
    int32_t partition_id{};    // used by SampleToNodeMasterMessage
    //    int32_t num_columns{}; // used by: writeBuffersFromGPUTCP,
    //    readBuffersIntoGPUTCP, update everywhere! int32_t num_buffers{};
  };

protected:
  explicit Message(std::string const &messageToken,
                   uint32_t const &contextToken,
                   const blazingdb::transport::Node& sender_node)
      : node_{sender_node.address(), sender_node.id()} {
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

  blazingdb::transport::Node& getSenderNode() {
    return node_;
  }
  const MetaData &metadata() const { return metadata_; }
  MetaData &metadata() { return metadata_; }

protected:
  MetaData metadata_;
  blazingdb::transport::Node node_;

  BZ_INTERFACE(Message);
};


class GPUMessage : public Message {
public:
  using raw_buffer = std::tuple<std::vector<std::size_t>, std::vector<const char *>,
                                std::vector<ColumnTransport>, std::vector<std::unique_ptr<rmm::device_buffer>> >;

public:
  explicit GPUMessage(std::string const &messageToken,
                      uint32_t const &contextToken,
                      const blazingdb::transport::Node &sender_node)
      : Message{messageToken, contextToken, sender_node} {}

  virtual raw_buffer GetRawColumns() {
    assert(true);
  }

  BZ_INTERFACE(GPUMessage);
};

class ReceivedMessage : public Message  {
public:
  explicit ReceivedMessage(std::string const &messageToken,
                      uint32_t const &contextToken,
                      const blazingdb::transport::Node &sender_node,
                      bool is_sentinel = false)
      : Message{messageToken, contextToken, sender_node}, _is_sentinel{is_sentinel} {}

  bool is_sentinel() { return _is_sentinel; }

  BZ_INTERFACE(ReceivedMessage);

private:
  bool _is_sentinel;
};

}  // namespace transport
}  // namespace blazingdb
