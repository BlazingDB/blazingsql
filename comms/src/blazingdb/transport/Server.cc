/*
#include "blazingdb/transport/Server.h"
#include "blazingdb/concurrency/BlazingThread.h"
#include "blazingdb/network/TCPSocket.h"
#include "blazingdb/transport/io/reader_writer.h"

#include "blazingdb/network/TCPSocket.h"
#include "blazingdb/transport/MessageQueue.h"
#include <condition_variable>
#include <cuda_runtime_api.h>
#include <deque>
#include <functional>
#include <map>
#include <mutex>
#include <numeric>
#include <rmm/device_buffer.hpp>
#include <shared_mutex>
#include <string>
#include <thread>
#include <tuple>
#include <vector>

namespace blazingdb {
namespace transport {

void Server::registerEndPoint(const std::string & end_point) { end_points_.insert(end_point); }

void Server::registerDeviceDeserializerForEndPoint(MakeDeviceFrameCallback deserializer, const std::string & end_point) {
	device_deserializer_[end_point] = deserializer;
}

void Server::registerHostDeserializerForEndPoint(MakeHostFrameCallback deserializer, const std::string & end_point) {
	host_deserializer_[end_point] = deserializer;
}

void Server::registerContext(const uint32_t context_token) {
	std::unique_lock<std::shared_timed_mutex> lock(context_messages_mutex_);
	context_messages_map_.emplace(std::piecewise_construct, std::forward_as_tuple(context_token), std::tuple<>());
}

void Server::deregisterContext(const uint32_t context_token) {
	std::unique_lock<std::shared_timed_mutex> lock(context_messages_mutex_);
	auto it = context_messages_map_.find(context_token);
	if(it != context_messages_map_.end()) {
		context_messages_map_.erase(it);
	}
}

std::shared_ptr<ReceivedMessage> Server::getMessage(const uint32_t context_token, const std::string & messageToken) {
	MessageQueue & message_queue = context_messages_map_.at(context_token);
	return message_queue.getMessage(messageToken);
}

void Server::putMessage(const uint32_t context_token, std::shared_ptr<ReceivedMessage> & message) {
	std::shared_lock<std::shared_timed_mutex> lock(context_messages_mutex_);
	if(context_messages_map_.find(context_token) == context_messages_map_.end()) {
		// putMessage could be called before registerContext so we force the
		// registration of the context token.
		lock.unlock();
		registerContext(context_token);
		lock.lock();
	}

	MessageQueue & message_queue = context_messages_map_.at(context_token);
	message_queue.putMessage(message);
}

Server::MakeDeviceFrameCallback Server::getDeviceDeserializationFunction(const std::string & endpoint) {
	const auto & iterator = device_deserializer_.find(endpoint);
	if(iterator == device_deserializer_.end()) {
		throw std::runtime_error("deserializer not found: " + endpoint);
	}
	return iterator->second;
}

Server::MakeHostFrameCallback Server::getHostDeserializationFunction(const std::string & endpoint) {
	const auto & iterator = host_deserializer_.find(endpoint);
	if(iterator == host_deserializer_.end()) {
		throw std::runtime_error("deserializer not found: " + endpoint);
	}
	return iterator->second;
}

template <typename MetadataType>
MetadataType read_metadata(void * fd) {
	MetadataType metadata;
	blazingdb::transport::io::readFromSocket(fd, (char *) &metadata, sizeof(MetadataType));
	return metadata;
}

namespace {


class ServerTCP : public Server {
public:
	ServerTCP(unsigned short port) : server_socket{port} {}

	void SetDevice(int gpuId) override { this->gpuId = gpuId; }

	virtual void Run() override;

	void Close() override { this->server_socket.close(); }

	~ServerTCP() {
		try {
			thread.join();
		} catch(std::exception & e) {
			std::cout << "ServerTCP::exception: " << e.what() << std::endl;
		}
	}

protected:

	blazingdb::network::TCPServerSocket server_socket;

	BlazingThread thread;

	int gpuId{0};
};
Message::MetaData collect_last_event(void * socket, Server * server) {
	zmq::socket_t * socket_ptr = (zmq::socket_t *) socket;
	Message::MetaData message_metadata = read_metadata<Message::MetaData>(socket);

	zmq::message_t local_message;
	auto success = socket_ptr->recv(local_message);
	if(success.value() == false || local_message.size() == 0) {
		throw zmq::error_t();
	}
	std::string ok_message(static_cast<char *>(local_message.data()), local_message.size());

	assert(ok_message == "OK");
	blazingdb::transport::io::writeToSocket(socket, "END", 3, false);

	return message_metadata;
}

template <typename buffer_container_type = std::vector<rmm::device_buffer>>
std::tuple<Message::MetaData, Address::MetaData, std::vector<ColumnTransport>, buffer_container_type>
collect_gpu_message(
	void * socket, int gpuId, void (*read_tpc_message)(std::vector<std::size_t>, void *, int, buffer_container_type &)) {
	zmq::socket_t * socket_ptr = (zmq::socket_t *) socket;
	// begin of message
	Message::MetaData message_metadata = read_metadata<Message::MetaData>(socket);
	Address::MetaData address_metadata = read_metadata<Address::MetaData>(socket);

	// read columns (gpu buffers)
	auto column_offset_size = read_metadata<int32_t>(socket);
	std::vector<ColumnTransport> column_offsets(column_offset_size);
	blazingdb::transport::io::readFromSocket(
		socket, (char *) column_offsets.data(), column_offset_size * sizeof(ColumnTransport));

	auto buffer_sizes_size = read_metadata<int32_t>(socket);
	std::vector<std::size_t> buffer_sizes(buffer_sizes_size);
	blazingdb::transport::io::readFromSocket(socket, (char *) buffer_sizes.data(), buffer_sizes_size * sizeof(std::size_t));

	buffer_container_type raw_columns;
	read_tpc_message(buffer_sizes, socket, gpuId, raw_columns);

	int data_past_topic{0};
	auto data_past_topic_size{sizeof(data_past_topic)};
	socket_ptr->getsockopt(ZMQ_RCVMORE, &data_past_topic, &data_past_topic_size);
	if(data_past_topic == 0 || data_past_topic_size == 0) {
		throw std::runtime_error("ZQMServer: No data inside message.");
	}
	// receive the ok
	zmq::message_t local_message;
	auto success = socket_ptr->recv(local_message);
	if(success.value() == false || local_message.size() == 0) {
		throw std::runtime_error("ZQMServer: success.value() == false");
	}

	std::string ok_message(static_cast<char *>(local_message.data()), local_message.size());
	assert(ok_message == "OK");
	blazingdb::transport::io::writeToSocket(socket, "END", 3, false);
	// end of message
	return std::make_tuple(message_metadata, address_metadata, column_offsets, raw_columns);
}

void ServerTCP::Run() {
	thread = BlazingThread([this]() {
		server_socket.run([this](void * socket) {
			try {
				zmq::socket_t * socket_ptr = (zmq::socket_t *) socket;
				zmq::message_t message_topic;
				auto success = socket_ptr->recv(message_topic);
				if(success.value() == false || message_topic.size() == 0) {
					throw zmq::error_t();
				}
				std::string message_topic_str(static_cast<char *>(message_topic.data()), message_topic.size());
				if(message_topic_str == "LAST") {
					collect_last_event(socket, this);
				} else if(message_topic_str == "GPUS") {
					Message::MetaData message_metadata;
					Address::MetaData address_metadata;
					std::vector<ColumnTransport> column_offsets;
					std::vector<rmm::device_buffer> raw_columns;

					std::tie(message_metadata, address_metadata, column_offsets, raw_columns) = collect_gpu_message(
						socket, gpuId, &blazingdb::transport::io::readBuffersIntoGPUTCP);

					std::string messageToken = message_metadata.messageToken;
					auto deserialize_function =
						this->getDeviceDeserializationFunction(messageToken.substr(0, messageToken.find('_')));
					std::shared_ptr<ReceivedMessage> message =
						deserialize_function(message_metadata, address_metadata, column_offsets, raw_columns);
					assert(message != nullptr);
					this->putMessage(message->metadata().contextToken, message);
				}
			} catch(const std::runtime_error & exception) {
				std::cerr << "[ERROR] " << exception.what() << std::endl;
				// TODO: write failure
				throw exception;
			}
		});
	});
	std::this_thread::yield();
}


class ServerForBatchProcessing : public ServerTCP {
public:
	ServerForBatchProcessing(unsigned short port) : ServerTCP(port) {}

	void Run() override {
		thread = BlazingThread([this]() {
			server_socket.run([this](void * socket) {
				try {
					zmq::socket_t * socket_ptr = (zmq::socket_t *) socket;
					zmq::message_t message_topic;
					auto success = socket_ptr->recv(message_topic);
					if(success.value() == false || message_topic.size() == 0) {
						throw zmq::error_t();
					}
					std::string message_topic_str(static_cast<char *>(message_topic.data()), message_topic.size());
					if(message_topic_str == "LAST") {
						auto message_metadata = collect_last_event(socket, this);

						std::string messageToken = message_metadata.messageToken;
						uint32_t contextToken = message_metadata.contextToken;
						blazingdb::transport::Node tmp_node;

						auto sentinel_message = std::make_shared<ReceivedMessage>(messageToken, contextToken, tmp_node, true);
						this->putMessage(contextToken, sentinel_message);
					
					} else if(message_topic_str == "GPUS") {
						Message::MetaData message_metadata;
						Address::MetaData address_metadata;
						std::vector<ColumnTransport> column_offsets;
						std::vector<Buffer> raw_columns;

						std::tie(message_metadata, address_metadata, column_offsets, raw_columns) =
							collect_gpu_message<std::vector<Buffer>>(
								socket, gpuId, blazingdb::transport::io::readBuffersIntoCPUTCP);
						std::string messageToken = message_metadata.messageToken;
						auto deserialize_function =
						this->getHostDeserializationFunction(messageToken.substr(0, messageToken.find('_')));
						std::shared_ptr<ReceivedMessage> message =
							deserialize_function(message_metadata, address_metadata, column_offsets, std::move(raw_columns));
						assert(message != nullptr);
						uint32_t contextToken = message->metadata().contextToken; 
						this->putMessage(contextToken, message);
						
					}
				} catch(const std::runtime_error & exception) {
					std::cerr << "[ERROR] " << exception.what() << std::endl;
					// TODO: write failure
					throw exception;
				}
			});
		});
		std::this_thread::yield();
	} 
};

}  // namespace

std::unique_ptr<Server> Server::TCP(unsigned short port) { return std::unique_ptr<Server>(new ServerTCP(port)); }

std::unique_ptr<Server> Server::BatchProcessing(unsigned short port) {
	return std::unique_ptr<Server>(new ServerForBatchProcessing(port));
}

}  // namespace transport
}  // namespace blazingdb
*/