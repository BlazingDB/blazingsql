#pragma once

#include <blazingdb/transport/Message.h>
#include <blazingdb/manager/NodeDataMessage.h>
#include <blazingdb/transport/Status.h>
#include <memory> 

namespace ral {
namespace communication {
namespace network {
    using Node = blazingdb::transport::Node;
    using GPUMessage = blazingdb::transport::GPUMessage;
    using Message = blazingdb::transport::Message;

    class Client {
    public:
        using Status = blazingdb::transport::Status;

    public:
        static Status send(const Node& node, GPUMessage& message);

        static Status sendNodeData(std::string ip, int16_t port, Message& message);

        static void closeConnections();
    };

} // namespace network
} // namespace communication
} // namespace ral
