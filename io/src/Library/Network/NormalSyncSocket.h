#ifndef SRC_LIBRARY_NETWORK_NORMALSYNCSOCKET_H_
#define SRC_LIBRARY_NETWORK_NORMALSYNCSOCKET_H_

#include "Library/Network/GenericSocket.h"
#include <boost/asio.hpp>

namespace Library {
namespace Network {
class NormalSyncSocket : public GenericSocket {
public:
	NormalSyncSocket();

	~NormalSyncSocket();

public:
	NormalSyncSocket(NormalSyncSocket &&) = delete;

	NormalSyncSocket(const NormalSyncSocket &) = delete;

	NormalSyncSocket & operator=(NormalSyncSocket &&) = delete;

	NormalSyncSocket & operator=(const NormalSyncSocket &) = delete;

public:
	void connect(const char * host, const char * port) override;

	void write(std::string && data) override;

	void write(const std::string & data) override;

private:
	boost::asio::io_service io_service;
	boost::asio::ip::tcp::socket socket;
};
}  // namespace Network
}  // namespace Library

#endif
