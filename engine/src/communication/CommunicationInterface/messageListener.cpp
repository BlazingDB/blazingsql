#include "messageListener.hpp"
#include <sys/socket.h>

#include "CodeTimer.h"
#include <mutex>

namespace comm {

using namespace fmt::literals;

ctpl::thread_pool<BlazingThread> & message_listener::get_pool(){
	return pool;
}

std::map<std::string, comm::node> message_listener::get_node_map(){
	return _nodes_info_map;
}

void poll_for_frames(std::shared_ptr<message_receiver> receiver,
		                 ucp_tag_t tag,
                     ucp_worker_h ucp_worker,
                     const std::size_t request_size){
	
	try {
		blazing_ucp_tag message_tag = *reinterpret_cast<blazing_ucp_tag *>(&tag);

		if (receiver->num_buffers() == 0) {
            ucx_message_listener::get_instance()->remove_receiver(tag);
			receiver->finish();
			return;
		}

		for (size_t buffer_id = 0; buffer_id < receiver->num_buffers(); buffer_id++) {
			receiver->allocate_buffer(buffer_id);

				message_tag.frame_id = buffer_id + 1;

			char *request = new char[request_size];
			ucs_status_t status = ucp_tag_recv_nbr(ucp_worker,
												receiver->get_buffer(buffer_id),
												receiver->buffer_size(buffer_id),
												ucp_dt_make_contig(1),
												*reinterpret_cast<ucp_tag_t *>(&message_tag),
												acknownledge_tag_mask,
												request + request_size);

				if (!UCS_STATUS_IS_ERR(status)) {
					status = ucp_request_check_status(request + request_size);
					if(status == UCS_OK){
						auto receiver = ucx_message_listener::get_instance()->get_receiver(tag & message_tag_mask);
						receiver->confirm_transmission();
						if (receiver->is_finished()) {
							ucx_message_listener::get_instance()->remove_receiver(tag & message_tag_mask);
						}
						delete request;
					}else{
						ucp_progress_manager::get_instance()->add_recv_request(request, [tag](){
						auto receiver = ucx_message_listener::get_instance()->get_receiver(tag & message_tag_mask);
						receiver->confirm_transmission();
						if (receiver->is_finished()) {
							ucx_message_listener::get_instance()->remove_receiver(tag & message_tag_mask);
								
						}
					},status);
					}
					
				} else {
					delete request;
					throw std::runtime_error("Immediate Communication error in poll_for_frames.");
				}
		}
	} catch(std::exception & e){
        std::shared_ptr<spdlog::logger> logger = spdlog::get("batch_logger");
        if (logger){
            logger->error("|||{info}|||||",
                    "info"_a="ERROR in poll_for_frames. What: {}"_format(e.what()));
        }
        throw;
    }

}


void recv_begin_callback_c(std::shared_ptr<ucp_tag_recv_info_t> info, std::shared_ptr<std::vector<char> > data_buffer, size_t request_size, std::shared_ptr<ral::cache::CacheMachine> input_cache) {

	auto message_listener = ucx_message_listener::get_instance();

   auto fwd = message_listener->get_pool().push([&message_listener, info, data_buffer, request_size, input_cache](int /*thread_id*/) {

   auto receiver = std::make_shared<message_receiver>(message_listener->get_node_map(), *data_buffer, input_cache);

		message_listener->add_receiver(info->sender_tag, receiver);

		poll_for_frames(receiver, info->sender_tag, message_listener->get_worker(), request_size);
	});
	try{
		fwd.get();
	}catch(const std::exception &e){
		std::cerr << "Error in recv_begin_callback_c: " << e.what() << std::endl;
		throw;
	}
}


void tcp_message_listener::start_polling() {
	if(!polling_started) {
		polling_started = true;
		int socket_fd;

		struct sockaddr_in server_address;

		// socket create and verification
		socket_fd = socket(AF_INET, SOCK_STREAM, 0);
		if(socket_fd == -1) {
			throw std::runtime_error("Couldn't allocate socket.");
		}

		bzero(&server_address, sizeof(server_address));


		server_address.sin_family = AF_INET;
		server_address.sin_addr.s_addr = htonl(INADDR_ANY);
		server_address.sin_port = htons(_port);

		if(bind(socket_fd, (struct sockaddr *) &server_address, sizeof(server_address)) != 0) {
			throw std::runtime_error("Could not bind to socket.");
		}

		// Now server is ready to listen and verification
		if(listen(socket_fd, 4096) != 0) {
			throw std::runtime_error("Could not listen on socket.");
		}
		auto thread = std::thread([this, socket_fd] {
			
			struct sockaddr_in client_address;
			socklen_t len;
			int connection_fd;
			// TODO: be able to stop this thread from running when the engine is killed
			while(true) {
				int accepted_conection = (connection_fd = accept(socket_fd, (struct sockaddr *) &client_address, &len));
				if(accepted_conection == -1){
                        std::shared_ptr<spdlog::logger> logger = spdlog::get("batch_logger");
						if (logger){
							logger->error("|||{info}|||||",
									"info"_a="ERROR in message_listener::run_polling() calling except. errno: {}"_format(errno));
						}
						std::this_thread::sleep_for(std::chrono::milliseconds(10));
					continue;
				}
				pool.push([this, connection_fd](int /*thread_num*/) {
					try{
						CodeTimer timer;
						cudaStream_t stream = 0;
				
						size_t message_size;
						io::read_from_socket(connection_fd, &message_size, sizeof(message_size));
						std::vector<char> data(message_size);
						io::read_from_socket(connection_fd, data.data(), message_size);
						// auto meta_read_time = timer.elapsed_time();
						// status_code success = status_code::OK;
						// io::write_to_socket(connection_fd, &success, sizeof(success));
						{
							auto receiver = std::make_shared<message_receiver>(_nodes_info_map, data, input_cache);

							//   auto receiver_time = timer.elapsed_time() - meta_read_time;
					
							size_t buffer_position = 0;
					
							//   size_t total_allocate_time = 0 ;
							//   size_t total_read_time = 0 ;
							//   size_t total_sync_time = 0;
							while(buffer_position < receiver->num_buffers()) {
								receiver->allocate_buffer(buffer_position, stream);
								void * buffer = receiver->get_buffer(buffer_position);
								size_t buffer_size = receiver->buffer_size(buffer_position);
								io::read_from_socket(connection_fd, buffer, buffer_size);

								buffer_position++;
							}
							close(connection_fd);
							//   auto duration = timer.elapsed_time();
							//   std::cout<<"Transfer duration before finish "<<duration <<" Throughput was "<<
							//   (( (float) total_size) / 1000000.0)/(((float) duration)/1000.0)<<" MB/s"<<std::endl;

							receiver->finish(stream);
						}
						cudaStreamSynchronize(stream);
						//	cudaStreamDestroy(stream);
					}catch(std::exception & e){
						close(connection_fd);
						std::shared_ptr<spdlog::logger> logger = spdlog::get("batch_logger");

						if (logger){
							logger->error("|||{info}|||||",
									"info"_a="ERROR in message_listener::run_polling(). What: {}"_format(e.what()));
						}
						
					}

				});
			}
		});
		thread.detach();
	}
}

void ucx_message_listener::poll_begin_message_tag(bool running_from_unit_test){

	if (!polling_started){
		polling_started = true;
		auto thread = std::thread([running_from_unit_test, this]{
			try {
				cudaSetDevice(0);

				for(;;){
					std::shared_ptr<ucp_tag_recv_info_t> info_tag = std::make_shared<ucp_tag_recv_info_t>();
					ucp_tag_message_h message_tag = nullptr;
					do {
						message_tag = ucp_tag_probe_nb(
							ucp_worker, 0ull, begin_tag_mask, 0, info_tag.get());

					}while(message_tag == nullptr);

						char * request = new char[_request_size];
						auto data_buffer = std::make_shared<std::vector<char>>(std::vector<char>(info_tag->length));
						
						auto status = ucp_tag_recv_nbr(ucp_worker,
							data_buffer->data(),
							info_tag->length,
							ucp_dt_make_contig(1),
							info_tag->sender_tag,
							acknownledge_tag_mask,
							request + _request_size);
						status = ucp_request_check_status(request + _request_size);
						if (!UCS_STATUS_IS_ERR(status)) {
							if(status == UCS_OK){
								recv_begin_callback_c(info_tag, data_buffer, _request_size, input_cache);
								delete request;
							}else{
								ucp_progress_manager::get_instance()->add_recv_request(
									request, 
									[info_tag, data_buffer=data_buffer, request_size=_request_size, input_message_cache=input_cache]() { 
										recv_begin_callback_c(info_tag, data_buffer, request_size, input_message_cache); }
									,status);
							}
						} else {
							throw std::runtime_error("Immediate Communication error in poll_begin_message_tag.");
						}
				}
			} catch(std::exception & e){
                std::shared_ptr<spdlog::logger> logger = spdlog::get("batch_logger");
				if (logger){
					logger->error("|||{info}|||||",
							"info"_a="ERROR in ucx_message_listener::poll_begin_message_tag. What: {}"_format(e.what()));
				}
				throw;
   			}
			

		});
		thread.detach();
	}
}


void ucx_message_listener::add_receiver(ucp_tag_t tag,std::shared_ptr<message_receiver> receiver){
	std::lock_guard<std::mutex> lock(this->receiver_mutex);
	tag_to_receiver[tag] = receiver;
}

std::shared_ptr<message_receiver> ucx_message_listener::get_receiver(ucp_tag_t tag) {
	std::lock_guard<std::mutex> lock(this->receiver_mutex);
	return tag_to_receiver.at(tag);
}

void ucx_message_listener::remove_receiver(ucp_tag_t tag){
	std::lock_guard<std::mutex> lock(this->receiver_mutex);
	if(tag_to_receiver.find(tag) != tag_to_receiver.end()){

        #if 0
        // send acknowledgment, currently not used
		auto receiver = tag_to_receiver[tag];
		reinterpret_cast<blazing_ucp_tag *>(&tag)->frame_id = 0xFFFF;
		char * buffer = new char[40];
		auto node_id = ral::communication::CommunicationData::getInstance().getSelfNode().id();
		memcpy(buffer, node_id.data(),node_id.length());
		buffer[node_id.length()] = 0;

		char *request = new char[_request_size];
        auto status = ucp_tag_send_nbr(receiver->get_sender_node().get_ucp_endpoint(),
                                            buffer,
                                            40,
                                            ucp_dt_make_contig(1),
                                            tag,
                                            request + _request_size);
		
		if ((status >= UCS_OK)) {
			//no callback needed for this
			ucp_progress_manager::get_instance()->add_send_request(request, [buffer](){delete buffer; },status);
		} else {
			throw std::runtime_error("Immediate Communication error in send_impl.");
		}
        #endif
		tag_to_receiver.erase(tag);
    }
        

}



ucp_worker_h ucx_message_listener::get_worker(){
	return ucp_worker;
}

ucx_message_listener * ucx_message_listener::instance = nullptr;
tcp_message_listener * tcp_message_listener::instance = nullptr;

ucx_message_listener::ucx_message_listener(ucp_context_h context, ucp_worker_h worker, const std::map<std::string, comm::node>& nodes, int num_threads, std::shared_ptr<ral::cache::CacheMachine> input_cache) :
	message_listener(nodes, num_threads,input_cache), ucp_worker{worker}
{
	try {
		ucp_context_attr_t attr;
		attr.field_mask = UCP_ATTR_FIELD_REQUEST_SIZE;
		ucs_status_t status = ucp_context_query(context, &attr);
		if (status != UCS_OK)	{
			throw std::runtime_error("Error calling ucp_context_query");
		}
		_request_size = attr.request_size;
		ucp_progress_manager::get_instance(worker,_request_size);
	} catch(std::exception & e){
        std::shared_ptr<spdlog::logger> logger = spdlog::get("batch_logger");
        if (logger){
            logger->error("|||{info}|||||",
                    "info"_a="ERROR in ucx_message_listener::ucx_message_listener. What: {}"_format(e.what()));
        }
        throw;
    }
}

tcp_message_listener::tcp_message_listener(const std::map<std::string, comm::node>& nodes,int port, int num_threads, std::shared_ptr<ral::cache::CacheMachine> input_cache) : message_listener{nodes,num_threads,input_cache}, _port{port} {

}

void ucx_message_listener::initialize_message_listener(ucp_context_h context, ucp_worker_h worker, const std::map<std::string, comm::node>& nodes, int num_threads, std::shared_ptr<ral::cache::CacheMachine> input_cache){
	if(instance == NULL) {
		instance = new ucx_message_listener(context, worker, nodes, num_threads, input_cache);
	}
}

void tcp_message_listener::initialize_message_listener(const std::map<std::string, comm::node>& nodes, int port, int num_threads, std::shared_ptr<ral::cache::CacheMachine> input_cache){
	if(instance == NULL){
		instance = new tcp_message_listener(nodes,port,num_threads,input_cache);
	}
}

void ucx_message_listener::start_polling(){
	if (!polling_started){
   		poll_begin_message_tag(false);
	}
}

ucx_message_listener * ucx_message_listener::get_instance() {
	if(instance == NULL) {
		throw std::runtime_error("ERROR: ucx_message_listener::get_instance() had a NULL instance");
	}
	return instance;
}

tcp_message_listener * tcp_message_listener::get_instance() {
	if(instance == NULL) {
		throw std::runtime_error("ERROR: tcp_message_listener::get_instance() had a NULL instance");
	}
	return instance;
}

}//namespace comm
