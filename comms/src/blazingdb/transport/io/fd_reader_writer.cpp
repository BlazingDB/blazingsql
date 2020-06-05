#include "blazingdb/transport/io/fd_reader_writer.h"
#include <netinet/in.h>
#include <unistd.h>
#include <cassert>
#include <iostream>
#include <queue>
#include <thread>
#include <zmq.hpp>
#include "blazingdb/transport/ColumnTransport.h"

namespace blazingdb {
namespace transport {
namespace io {

void readFromSocket(void* fileDescriptor, char* buf, size_t nbyte) {
  zmq::socket_t* socket = (zmq::socket_t*)fileDescriptor;
  zmq::message_t msg;
  try {
    socket->recv(&msg);
    if (buf == nullptr){
      throw std::runtime_error("Attempted to write into a non initialized Pinned memory buffer when reading from Socket");
    }
    memcpy(buf, msg.data(), nbyte);
  }catch (zmq::error_t & err) {
    throw err;
  }catch ( std::runtime_error& re){
    throw re;
  }
}

void writeToSocket(void* fileDescriptor, const char* buf, size_t nbyte, bool more) {
  zmq::socket_t* socket = (zmq::socket_t*)fileDescriptor;
  zmq::message_t message(nbyte);
  try {
    if (buf == nullptr){
      throw std::runtime_error("Attempted to read from a non initialized Pinned memory buffer when writing to Socket");
    }
    memcpy(message.data(), buf, nbyte);
    socket->send(message, more ? ZMQ_SNDMORE : 0);
  }catch (zmq::error_t & err) {
    throw err;
  }catch ( std::runtime_error& re){
    throw re;
  }
}

}  // namespace io
}  // namespace transport
}  // namespace blazingdb
