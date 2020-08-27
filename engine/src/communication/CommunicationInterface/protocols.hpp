#pragma once

#include <blazingdb/transport/ColumnTransport.h>
#include <atomic>

#include "bufferTransport.hpp"
#include "messageReceiver.hpp"
#include "node.hpp"
#include "utilities/ctpl_stl.h"

#include "execution_graph/logic_controllers/taskflow/graph.h"

namespace comm {

enum blazing_protocol
{
    ucx,
    tcp
};

class ucp_nodes_info
{
public:
	static ucp_nodes_info & getInstance();

	void init(const std::map<std::string, node> & nodes_map);
    node get_node(const std::string& id);

private:
    ucp_nodes_info() = default;
	ucp_nodes_info(ucp_nodes_info &&) = delete;
	ucp_nodes_info(const ucp_nodes_info &) = delete;
	ucp_nodes_info & operator=(ucp_nodes_info &&) = delete;
	ucp_nodes_info & operator=(const ucp_nodes_info &) = delete;

    std::map<std::string, node> _id_to_node_info_map;
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
    ucx_buffer_transport(
        ucp_worker_h origin_node,
        std::vector<node> destinations,
		ral::cache::MetadataDictionary metadata,
		std::vector<size_t> buffer_sizes,
		std::vector<blazingdb::transport::ColumnTransport> column_transports,
        int ral_id);
    ~ucx_buffer_transport();

    void send_begin_transmission() override;
    void wait_until_complete() override;
    void wait_for_begin_transmission();
    void increment_frame_transmission();
    void increment_begin_transmission();
    void recv_begin_transmission_ack();

protected:
    void send_impl(const char * buffer, size_t buffer_size) override;

private:

    std::atomic<size_t> transmitted_begin_frames; /**<  The number of begin_transmission messages sent */
    std::atomic<size_t> transmitted_frames; /**< The number of frames transmitted */
    std::mutex mutex;
    std::condition_variable completion_condition_variable;
    ucp_worker_h origin_node;
    std::vector<node> destinations;
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
};




static const ucp_tag_t begin_tag_mask = 0xFFFF000000000000;
static const ucp_tag_t message_tag_mask = 0x0000FFFFFFFFFFFF;
static const ucp_tag_t acknownledge_tag_mask = 0xFFFFFFFFFFFFFFFF;


class ucx_message_listener {
public:

    static void initialize_message_listener(ucp_worker_h worker, int num_threads);
    static ucx_message_listener * get_instance();
    void poll_begin_message_tag();
    void add_receiver(ucp_tag_t tag,std::shared_ptr<message_receiver> receiver);
    void remove_receiver(ucp_tag_t tag);
    void increment_frame_receiver(ucp_tag_t tag);
    ucp_worker_h get_worker();
    ctpl::thread_pool<BlazingThread> & get_pool();
private:
    ucx_message_listener(ucp_worker_h worker, int num_threads);
	ctpl::thread_pool<BlazingThread> pool;
    void poll_message_tag(ucp_tag_t tag, ucp_tag_t mask);
    ucp_worker_h ucp_worker;
    std::map<ucp_tag_t,std::shared_ptr<message_receiver> > tag_to_receiver;
	static ucx_message_listener * instance;
};

} // namespace comm
