#pragma once

#include <map>
#include <vector>
#include <exception>
#include <blazingdb/io/Util/StringUtil.h>
#include <transport/ColumnTransport.h>

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
	const T * byte_pointer = reinterpret_cast<const T *>(input);
	return *byte_pointer;
}

template <typename T>
std::vector<T> vector_from_byte_vector(const char * input, size_t length) {
	const T * byte_pointer = reinterpret_cast<const T *>(input);
	return std::vector<T>(byte_pointer,byte_pointer + length);
}

std::vector<char> serialize_metadata_and_transports_and_buffer_sizes(const ral::cache::MetadataDictionary & metadata,
                                                    const std::vector<blazingdb::transport::ColumnTransport> & column_transports,
                                                    const std::vector<size_t> buffer_sizes);

std::tuple<ral::cache::MetadataDictionary, std::vector<blazingdb::transport::ColumnTransport> , std::vector<size_t> > get_metadata_and_transports_and_buffer_sizes_from_bytes(std::vector<char> data);

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
		std::vector<blazingdb::transport::ColumnTransport> column_transports,
		std::vector<node> destinations);
	virtual ~buffer_transport();

  virtual void send_begin_transmission() = 0;

	/**
	 * @brief Sends a chunk of bytes throught a transport protocol
	 *
	 * @param buffer Pointer to the byte buffer that will be send
	 * @param buffer_size The buffer size
	 */
  void send(const char * buffer, size_t buffer_size);

	/**
	 * @brief Waits until all the data is sents
	 */
	void wait_until_complete();

	void wait_for_begin_transmission();
	virtual void increment_frame_transmission();
	virtual void increment_begin_transmission();
protected:
	virtual void send_impl(const char * buffer, size_t buffer_size) = 0;

	std::vector<blazingdb::transport::ColumnTransport> column_transports;
	ral::cache::MetadataDictionary metadata;
	std::vector<size_t> buffer_sizes;
	size_t buffer_sent = 0;

	std::atomic<size_t> transmitted_begin_frames; /**<  The number of begin_transmission messages sent */
	std::atomic<size_t> transmitted_frames; /**< The number of frames transmitted */
	std::mutex mutex;
	std::condition_variable completion_condition_variable;
	std::vector<node> destinations;
};


}  // namespace comm
