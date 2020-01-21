#pragma once

#include <blazingdb/transport/Message.h>
#include <blazingdb/transport/Server.h>
#include <thread>

namespace ral {
namespace communication {
namespace network {
namespace experimental {

using CommServer = blazingdb::transport::experimental::Server;
using ContextToken = uint32_t;
using MessageTokenType = std::string;
using GPUMessage = blazingdb::transport::experimental::GPUMessage;
using GPUReceivedMessage = blazingdb::transport::experimental::GPUReceivedMessage;

class Server {
public:
	static void start(unsigned short port = 8000);

	static void close();

	static Server & getInstance();

private:
	Server();

public:
	~Server();

public:
	void registerContext(const ContextToken context_token);
	void deregisterContext(const ContextToken context_token);

public:
	std::shared_ptr<GPUReceivedMessage> getMessage(const ContextToken & token_value, const MessageTokenType & messageToken);

private:
	Server(Server &&) = delete;

	Server(const Server &) = delete;

	Server & operator=(Server &&) = delete;

	Server & operator=(const Server &) = delete;

private:
	void setEndPoints();

private:
	std::thread thread;
	std::shared_ptr<CommServer> comm_server;

private:
	static unsigned short port_;
};

}  // namespace experimental
}  // namespace network
}  // namespace communication
}  // namespace ral
