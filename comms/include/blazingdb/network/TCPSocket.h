#pragma once
#include <cstring>
#include <functional>
#include <iostream>
#include <shared_mutex>
#include <stdexcept>
#include <string>
#include <thread>
#include "blazingdb/transport/common/macros.hpp"
#include "blazingdb/transport/io/fd_reader_writer.h"

#include <zmq.hpp>

namespace blazingdb {
namespace network {

class TCPServerSocket {
public:
  TCPServerSocket(int tcp_port) : context(1) {
    try {
      socket = zmq::socket_t(context, ZMQ_REP);
      auto connection = "tcp://*:" + std::to_string(tcp_port);
      std::cout << "listening: " << connection << std::endl;
      int linger = -1;
      socket.setsockopt(ZMQ_LINGER, &linger, sizeof(linger));
      socket.bind(connection);

    } catch (std::exception &e) {
      std::cerr << e.what() << std::endl;
    }
  }

  void run(std::function<void(void *)> handler) {
    while (context) {
      if (socket.connected()) {
        handler((void *)&socket);

        //        uint64_t more_part;
        //        size_t more_size = sizeof(more_part);
        //
        //        socket.getsockopt(ZMQ_RCVMORE, &more_part, &more_size);

      } else {
        break;
      }
    }
  }
  void close() {
    socket.close();
    context.close();
  }

private:
  zmq::context_t context;
  zmq::socket_t socket;
  std::function<void(int)> handler;
};

class TCPClientSocket {
public:
  TCPClientSocket(const std::string &tcp_host, int tcp_port) : context(1) {
    try {
      socket = zmq::socket_t(context, ZMQ_REQ);
      auto connection = "tcp://" + tcp_host + ":" + std::to_string(tcp_port);
      int linger = -1;
      socket.setsockopt(ZMQ_LINGER, &linger, sizeof(linger));
      socket.connect(connection);
    } catch (std::exception &e) {
      std::cerr << e.what() << std::endl;
    }
  }

  void close() {
    socket.close();
    context.close();
  }

  void *fd() { return (void *)&socket; }

private:
  zmq::context_t context;
  zmq::socket_t socket;
};

}  // namespace network
}  // namespace blazingdb
