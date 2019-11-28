#include "Library/Network/NormalSyncSocket.h"

namespace Library {
    namespace Network {
        NormalSyncSocket::NormalSyncSocket()
        : socket(io_service) {
        }

        NormalSyncSocket::~NormalSyncSocket() {
        }

        void NormalSyncSocket::connect(const char* host, const char* port) {
            boost::asio::ip::tcp::resolver resolver(io_service);
            boost::asio::ip::tcp::resolver::query query(boost::asio::ip::tcp::v4(), host, port);
            boost::asio::ip::tcp::resolver::iterator iterator = resolver.resolve(query);
            boost::asio::connect(socket, iterator);
        }

        void NormalSyncSocket::write(std::string&& data) {
            boost::asio::write(socket, boost::asio::buffer(data));
        }

        void NormalSyncSocket::write(const std::string& data) {
            boost::asio::write(socket, boost::asio::buffer(data));
        }
    }
}
