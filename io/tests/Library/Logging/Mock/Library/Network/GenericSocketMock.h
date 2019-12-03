#ifndef TEST_MOCK_LIBRARY_NETWORK_GENERICSOCKETMOCK_H_
#define TEST_MOCK_LIBRARY_NETWORK_GENERICSOCKETMOCK_H_

#include "Library/Network/GenericSocket.h"
#include <gmock/gmock.h>

namespace BlazingTest {
namespace Library {
namespace Network {
struct GenericSocketMock : public ::Library::Network::GenericSocket {
	MOCK_METHOD2(connect, void(const char * host, const char * port));

	MOCK_METHOD1(write, void(std::string && data));

	MOCK_METHOD1(write, void(const std::string & data));
};
}  // namespace Network
}  // namespace Library
}  // namespace BlazingTest

#endif
