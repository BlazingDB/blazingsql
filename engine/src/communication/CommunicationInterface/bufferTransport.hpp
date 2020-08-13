#pragma once

#include <map>
#include <vector>
#include <exception>

#include "execution_graph/logic_controllers/CacheMachine.h"
#include "blazingdb/transport/Node.h"

namespace comm {

namespace detail {

template <typename T>
std::vector<char> to_byte_vector(T input) {
	char * bytePointer = reinterpret_cast<char *>(&input);
	return std::vector<char>(bytePointer, bytePointer + sizeof(T));
}

template <typename T>
std::vector<char> vector_to_byte_vector(std::vector<T> input) {
	char * bytePointer = reinterpret_cast<char *>(input.data());
	return std::vector<char>(bytePointer, bytePointer + (sizeof(T) * input.size()));
}

template <typename T>
T from_byte_vector(std::vector<char> input) {
	T * bytePointer = reinterpret_cast<T *>(input.data());
	return *T;
}

} // namespace detail

class buffer_transport
{
public:
	buffer_transport(std::map<std::string, blazingdb::transport::Node> node_address_map,
		MetadataDictionary metadata,
		std::vector<size_t> buffer_sizes,
		std::vector<column_transport> column_transports)
		: column_transports{column_transports}, buffer_sizes{buffer_sizes}, metadata{metadata} {
		// iterate for workers this is destined for
		for(auto worker_id : StringUtil::split(metadata[ral::cache::WORKER_IDS_METADATA_LABEL], ",")) {
			if(node_address_map.find(worker_id) == node_address_map.end()) {
				throw std::exception();	 // TODO: make a real exception here
			}
			destinations.push_back(node_address_map[worker_id])
		}
	}

  virtual void send_begin_transmission() = 0;

  void send(const char * buffer, size_t buffer_size){
		send_implementation(buffer, buffer_size);
		buffer_sent++;
	}

  virtual void wait_until_complete() = 0;

protected:
	virtual void send_implementation(const char * buffer, size_t buffer_size) = 0;

	std::string make_begin_transmission() {
		// builds the cpu host buffer that we are going to send
		// first lets serialize and send metadata
		std::string metadata_buffer;
		for(auto it : metadata.get_values()) {
			metadata_buffer += it.first + ":" + it.second + "\n";
		}

		std::string buffer;
		buffer.append(to_byte_vector(metadata_buffer.size()));
		buffer.append(metadata_buffer);
		buffer.append(to_byte_vector(column_transports.size()));  // tells us how many befores will be sent
		buffer.append(vector_to_byte_vector(column_transports));

		return buffer;
	}

  std::vector<blazingdb::transport::Node> destinations; /**< The nodes that will be receiving these buffers */
	std::vector<column_transport> column_transports;
	ral::cache::MetadataDictionary metadata;
	std::vector<size_t> buffer_sizes;
	size_t buffer_sent = 0;
};

class buffer_tcp_transport : public buffer_transport {
public:
	void send_begin_transmission() override {

  }

	void send_implementation(const char * buffer, size_t buffer_size) override {
		// impl
	}

	void wait_until_complete() override {

  }

};

class buffer_ucx_transport : public buffer_transport {
public:
	void send_begin_transmission() override {

  }

	void send_implementation(const char * buffer, size_t buffer_size) override {
		// impl
	}

	void wait_until_complete() override {

  }

};

}  // namespace comm
