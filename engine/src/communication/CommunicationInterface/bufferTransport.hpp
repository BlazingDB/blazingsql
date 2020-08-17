#pragma once

#include <map>
#include <vector>
#include <exception>
#include <blazingdb/io/Util/StringUtil.h>

#include "node.hpp"
#include "execution_graph/logic_controllers/CacheMachine.h"

namespace comm {

namespace detail {

template <typename T>
std::vector<char> to_byte_vector(T input) {
	char * byte_pointer = reinterpret_cast<char *>(&input);
	return std::vector<char>(byte_pointer, byte_pointer + sizeof(T));
}

template <typename T>
std::vector<char> vector_to_byte_vector(std::vector<T> input) {
	char * byte_pointer = reinterpret_cast<char *>(input.data());
	return std::vector<char>(byte_pointer, byte_pointer + (sizeof(T) * input.size()));
}

template <typename T>
T from_byte_vector(const char * input) {
	T * byte_pointer = reinterpret_cast<T *>(input);
	return *byte_pointer;
}

template <typename T>
T vector_from_byte_vector(const char * input, size_t length) {
	T * byte_pointer = reinterpret_cast<T *>(input);
	return std::vector<T>(byte_pointer,byte_pointer + length);
}

std::pair<ral::cache::MetadataDictionary, std::vector<ColumnTransport> > get_metadata_and_transports_from_bytes(std::vector<char> data){
    size_t ptr_offset = 0;
	size_t metadata_buffer_size = from_byte_vector<size_t>(data.data());
	ptr_offset += sizeof(size_t);

	std::string metadata_buffer(
		data.data() + ptr_offset,
		data.data() + ptr_offset + metadata_buffer_size);
	ptr_offset += metadata_buffer_size;
	ral::cache::MetadataDictionary dictionary;
	for(auto metadata_item : StringUtill::split(metadata_buffer,"\n")){
		std::vector<std::string> key_value = StringUtill::split(metadata_buffer,":");
		dictionary.add_value(key_value[0],key_value[1]);
	}

	size_t column_transports_size = from_byte_vector<size_t>(
		data.data() + ptr_offset);
	ptr_offset += sizeof(size_t);
	auto column_transports = vector_from_byte_vector<ColumnTransport>(
		data.data() + ptr_offset, column_transports_size);
	return std::make_pair(dictionary,column_transports);
}

} // namespace detail


/**
 * @brief Base class used to send a chunk of bytes throught a transport protocol
 * e.g. TCP, UCP, etc
 *
 */
class buffer_transport
{
public:
	/**
	 * @brief Constructs a buffer_transport
	 *
	 * @param metadata This is information about how the message was routed and payloads that are used in
   * execution, planning, or physical optimizations. E.G. num rows in table, num partitions to be processed
	 * @param buffer_sizes A vector containing the sizes of the buffer
	 * @param column_transports A vector of ColumnTransport representing column metadata
	 */
	buffer_transport(ral::cache::MetadataDictionary metadata,
		std::vector<size_t> buffer_sizes,
		std::vector<blazingdb::transport::ColumnTransport> column_transports)
		: column_transports{column_transports}, buffer_sizes{buffer_sizes}, metadata{metadata} {
		// iterate for workers this is destined for

	}
	virtual ~buffer_transport();

  virtual void send_begin_transmission() = 0;

	/**
	 * @brief Sends a chunk of bytes throught a transport protocol
	 *
	 * @param buffer Pointer to the byte buffer that will be send
	 * @param buffer_size The buffer size
	 */
  void send(const char * buffer, size_t buffer_size){
		send_impl(buffer, buffer_size);
		buffer_sent++;
	}

	/**
	 * @brief Waits until all the data is send
	 */
  virtual void wait_until_complete() = 0;

protected:
	virtual void send_impl(const char * buffer, size_t buffer_size) = 0;

	/**
	 *
	 */
	std::vector<char> make_begin_transmission() {
		// builds the cpu host buffer that we are going to send
		// first lets serialize and send metadata
		std::string metadata_buffer;
		for(auto it : metadata.get_values()) {
			metadata_buffer += it.first + ":" + it.second + "\n";
		}

		std::vector<char> buffer, tmp_buffer;
		tmp_buffer = detail::to_byte_vector(metadata_buffer.size());
		buffer.insert(buffer.end(), tmp_buffer.begin(), tmp_buffer.end());

		buffer.insert(buffer.end(), metadata_buffer.begin(), metadata_buffer.end());

		tmp_buffer = detail::to_byte_vector(column_transports.size()); // tells us how many befores will be sent
		buffer.insert(buffer.end(), tmp_buffer.begin(), tmp_buffer.end());

		tmp_buffer = detail::vector_to_byte_vector(column_transports);
		buffer.insert(buffer.end(), tmp_buffer.begin(), tmp_buffer.end());

		return buffer;
	}
	std::vector<blazingdb::transport::ColumnTransport> column_transports;
	ral::cache::MetadataDictionary metadata;
	std::vector<size_t> buffer_sizes;
	size_t buffer_sent = 0;
};


}  // namespace comm
