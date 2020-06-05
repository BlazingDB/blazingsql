#include "blazingdb/transport/io/fd_reader_writer.h"
#include <netinet/in.h>
#include <unistd.h>
#include <cassert>
#include <iostream>
#include <queue>
#include <thread>
#include <zmq.hpp>
#include "blazingdb/transport/ColumnTransport.h"

#include <stdio.h>
#include <stdlib.h>
#include <execinfo.h>
#include <cxxabi.h>

/** Print a demangled stack backtrace of the caller function to FILE* out. */
static inline void print_stacktrace(FILE *out = stderr, unsigned int max_frames = 63)
{
    fprintf(out, "stack trace:\n");

    // storage array for stack trace address data
    void* addrlist[max_frames+1];

    // retrieve current stack addresses
    int addrlen = backtrace(addrlist, sizeof(addrlist) / sizeof(void*));

    if (addrlen == 0) {
	fprintf(out, "  <empty, possibly corrupt>\n");
	return;
    }

    // resolve addresses into strings containing "filename(function+address)",
    // this array must be free()-ed
    char** symbollist = backtrace_symbols(addrlist, addrlen);

    // allocate string which will be filled with the demangled function name
    size_t funcnamesize = 256;
    char* funcname = (char*)malloc(funcnamesize);

    // iterate over the returned symbol lines. skip the first, it is the
    // address of this function.
    for (int i = 1; i < addrlen; i++)
    {
	char *begin_name = 0, *begin_offset = 0, *end_offset = 0;

	// find parentheses and +address offset surrounding the mangled name:
	// ./module(function+0x15c) [0x8048a6d]
	for (char *p = symbollist[i]; *p; ++p)
	{
	    if (*p == '(')
		begin_name = p;
	    else if (*p == '+')
		begin_offset = p;
	    else if (*p == ')' && begin_offset) {
		end_offset = p;
		break;
	    }
	}

	if (begin_name && begin_offset && end_offset
	    && begin_name < begin_offset)
	{
	    *begin_name++ = '\0';
	    *begin_offset++ = '\0';
	    *end_offset = '\0';

	    // mangled name is now in [begin_name, begin_offset) and caller
	    // offset in [begin_offset, end_offset). now apply
	    // __cxa_demangle():

	    int status;
	    char* ret = abi::__cxa_demangle(begin_name,
					    funcname, &funcnamesize, &status);
	    if (status == 0) {
		funcname = ret; // use possibly realloc()-ed string
		fprintf(out, "  %s : %s+%s\n",
			symbollist[i], funcname, begin_offset);
	    }
	    else {
		// demangling failed. Output function name as a C function with
		// no arguments.
		fprintf(out, "  %s : %s()+%s\n",
			symbollist[i], begin_name, begin_offset);
	    }
	}
	else
	{
	    // couldn't parse the line? print the whole line.
	    fprintf(out, "  %s\n", symbollist[i]);
	}
    }

    free(funcname);
    free(symbollist);
}


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
      print_stacktrace();
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
      print_stacktrace();
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
