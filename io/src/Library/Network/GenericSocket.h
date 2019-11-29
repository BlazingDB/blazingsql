#ifndef SRC_LIBRARY_NETWORK_GENERICSOCKET_H_
#define SRC_LIBRARY_NETWORK_GENERICSOCKET_H_

#include <string>

namespace Library {
    namespace Network {
        class GenericSocket {
        public:
            virtual void connect(const char* host, const char* port) = 0;

            virtual void write(std::string&& data) = 0;

            virtual void write(const std::string& data) = 0;
        };
    }
}

#endif
