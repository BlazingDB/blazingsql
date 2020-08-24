#pragma once

#include <blazingdb/transport/Message.h>
#include <blazingdb/transport/Server.h>

namespace ral {
namespace communication {
namespace network {

using CommServer = blazingdb::transport::Server;
using ContextToken = uint32_t;
using MessageTokenType = std::string;
using GPUMessage = blazingdb::transport::GPUMessage;
using ReceivedMessage = blazingdb::transport::ReceivedMessage;
using HostCallback = blazingdb::transport::HostCallback;

class Server {
public:
	static void start(unsigned short port = 8000, bool use_batch_processing = false);




private:
	Server();

public:
	~Server();

public:
	void registerContext(const ContextToken context_token);
	void deregisterContext(const ContextToken context_token);

public:
	std::shared_ptr<ReceivedMessage> getMessage(const ContextToken & token_value, const MessageTokenType & messageToken);

	std::shared_ptr<ReceivedMessage> getHostMessage(const ContextToken & token_value, const MessageTokenType & messageToken);

private:
	Server(Server &&) = delete;

	Server(const Server &) = delete;

	Server & operator=(Server &&) = delete;

	Server & operator=(const Server &) = delete;

private:
	void setEndPoints();

private:
	std::shared_ptr<CommServer> comm_server;

private:
	static unsigned short port_;
	static bool use_batch_processing_;
};

}  // namespace network
}  // namespace communication
}  // namespace ral
