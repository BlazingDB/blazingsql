#pragma once

#include <transport/ColumnTransport.h>
#include <atomic>

#include "bufferTransport.hpp"
#include "messageReceiver.hpp"
#include "node.hpp"
#include "utilities/ctpl_stl.h"
#include <arpa/inet.h>
#include "execution_graph/logic_controllers/taskflow/graph.h"

namespace io{
    void read_from_socket(int socket_fd, void * data, size_t read_size);
    void write_to_socket(int socket_fd, const void * data, size_t read_size);
}


namespace comm {



class ucp_progress_manager{

public:

   	static ucp_progress_manager * get_instance(ucp_worker_h ucp_worker, size_t request_size);
    static ucp_progress_manager * get_instance();
    void add_recv_request(char * request, std::function<void()> callback, ucs_status_t status);
    void add_send_request(char * request, std::function<void()> callback, ucs_status_t status);
private:
   struct request_struct{
        char * request;
        std::function<void()> callback;
        bool operator <(const request_struct & other) const{
            return request < other.request;
        }
    };
    ucp_progress_manager(ucp_worker_h ucp_worker,size_t request_size);
   	ucp_progress_manager(ucp_progress_manager &&) = delete;
	ucp_progress_manager(const ucp_progress_manager &) = delete;
	ucp_progress_manager & operator=(ucp_progress_manager &&) = delete;
	ucp_progress_manager & operator=(const ucp_progress_manager &) = delete;
    size_t _request_size;
    std::mutex request_mutex;
    std::condition_variable cv;
    std::set<request_struct> send_requests;
    std::set<request_struct> recv_requests;
    ucp_worker_h ucp_worker;
    void check_progress();
};



enum class status_code {
	INVALID = -1,
	OK = 1,
	ERROR = 0
};


enum blazing_protocol
{
    ucx,
    tcp
};

class graphs_info
{
public:
	static graphs_info & getInstance();

	void register_graph(int32_t ctx_token, std::shared_ptr<ral::cache::graph> graph);
    void deregister_graph(int32_t ctx_token);

    std::shared_ptr<ral::cache::graph> get_graph(int32_t ctx_token);

private:
    graphs_info() = default;
	graphs_info(graphs_info &&) = delete;
	graphs_info(const graphs_info &) = delete;
	graphs_info & operator=(graphs_info &&) = delete;
	graphs_info & operator=(const graphs_info &) = delete;

    std::map<int32_t, std::shared_ptr<ral::cache::graph>> _ctx_token_to_graph_map;
};

/**
 * A class that can send a buffer via  ucx protocol
 */
class ucx_buffer_transport : public buffer_transport {
public:
    ucx_buffer_transport(size_t request_size,
        ucp_worker_h origin_node,
        std::vector<node> destinations,
		ral::cache::MetadataDictionary metadata,
		std::vector<size_t> buffer_sizes,
		std::vector<blazingdb::transport::ColumnTransport> column_transports,
        std::vector<ral::memory::blazing_chunked_buffer> chunked_buffers,
        int ral_id,
        bool require_acknowledge);
    ~ucx_buffer_transport();

    void send_begin_transmission() override;

protected:
    void send_impl(const char * buffer, size_t buffer_size) override;
    void receive_acknowledge();
	
private:

    ucp_worker_h origin_node;
    int ral_id;
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

    int message_id;

    size_t _request_size;
};


class tcp_buffer_transport : public buffer_transport {
public:

    tcp_buffer_transport(
        std::vector<node> destinations,
        ral::cache::MetadataDictionary metadata,
        std::vector<size_t> buffer_sizes,
        std::vector<blazingdb::transport::ColumnTransport> column_transports,
        std::vector<ral::memory::blazing_chunked_buffer> chunked_buffers,
        int ral_id,
        ctpl::thread_pool<BlazingThread> * allocate_copy_buffer_pool,
        bool require_acknowledge);
    ~tcp_buffer_transport();

    void send_begin_transmission() override;

protected:
    void send_impl(const char * buffer, size_t buffer_size) override;
    void receive_acknowledge();
	
private:
    int ral_id;
    int message_id;
    std::vector<int> socket_fds;
    ctpl::thread_pool<BlazingThread> * allocate_copy_buffer_pool;
    cudaStream_t stream;
};




static const ucp_tag_t begin_tag_mask =        0xFFFF000000000000;
static const ucp_tag_t message_tag_mask =      0x0000FFFFFFFFFFFF;
static const ucp_tag_t acknownledge_tag_mask = 0xFFFFFFFFFFFFFFFF;


} // namespace comm
