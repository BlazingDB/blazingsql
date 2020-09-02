#pragma once

#include <memory>
#include <map>

#include "utilities/ctpl_stl.h"
#include "blazingdb/concurrency/BlazingThread.h"
#include <ucp/api/ucp.h>
#include <ucp/api/ucp_def.h>
#include "messageReceiver.hpp"

namespace comm {

class message_listener{
public:
    message_listener(int num_threads) : pool{num_threads} {}
    virtual ~message_listener(){

    }
    virtual void start_polling() = 0;
    ctpl::thread_pool<BlazingThread> & get_pool();
protected:
    ctpl::thread_pool<BlazingThread> pool;
};

class tcp_message_listener : public message_listener {

public:
    void start_polling() override;
    virtual ~tcp_message_listener(){

    }
private:
    tcp_message_listener(int port, int num_threads);
    int _port;
};



class ucx_message_listener : public message_listener {
public:

    static void initialize_message_listener(ucp_worker_h worker, int num_threads);
    static ucx_message_listener * get_instance();
    void poll_begin_message_tag();
    void add_receiver(ucp_tag_t tag,std::shared_ptr<message_receiver> receiver);
    void remove_receiver(ucp_tag_t tag);
    void increment_frame_receiver(ucp_tag_t tag);
    ucp_worker_h get_worker();
    void start_polling() override;
private:
    ucx_message_listener(ucp_worker_h worker, int num_threads);
	virtual ~ucx_message_listener(){

    }
    void poll_message_tag(ucp_tag_t tag, ucp_tag_t mask);
    ucp_worker_h ucp_worker;
    std::map<ucp_tag_t,std::shared_ptr<message_receiver> > tag_to_receiver;
	static ucx_message_listener * instance;
};

} // namespace comm