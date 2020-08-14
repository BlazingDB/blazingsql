#pragma once

#include <blazingdb/transport/ColumnTransport.h>
#include <atomic>

#include "bufferTransport.hpp"
#include "node.hpp"

namespace comm {

/**
 * A class that can send a buffer via  ucx protocol
 */
class ucx_buffer_transport : public buffer_transport {
public:
    ucx_buffer_transport(
        node origin_node,
        std::vector<node> destinations,
		ral::cache::MetadataDictionary metadata,
		std::vector<size_t> buffer_sizes,
		std::vector<blazingdb::transport::ColumnTransport> column_transports);

    void send_begin_transmission() override;
    void wait_until_complete() override;
    void wait_for_begin_transmission();
    void increment_frame_transmission();
    void increment_begin_transmission();

protected:
    void send_impl(const char * buffer, size_t buffer_size) override;

private:
    std::atomic<size_t> transmitted_begin_frames; /**<  The number of begin_transmission messages sent */
    std::atomic<size_t> transmitted_frames; /**< The number of frames transmitted */
    std::mutex mutex;
    std::condition_variable completion_condition_variable;
    node origin_node;
    std::vector<node> destinations;
    /**
     * Generates message tag.
     * Generates a tag for the message where the first 4 bytes are our
     * message id. The next 2 bytes are our worker number.
     * The final 2 bytes are 00 and used for sending frame number
     * @return a ucp_tag_t where the first 6 bytes are unique to this worker
     */
    ucp_tag_t generate_message_tag();
    ucp_tag_t tag;  /**< The first 6 bytes are the actual tag the last two
                         indicate which frame this is. */
};

} // namespace comm
