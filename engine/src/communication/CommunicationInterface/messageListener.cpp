#include "messageListener.hpp"
#include <sys/socket.h>

namespace comm {

std::map<ucp_tag_t, std::pair<std::vector<char>, std::shared_ptr<ucp_tag_recv_info_t> > > tag_to_begin_buffer_and_info;


ctpl::thread_pool<BlazingThread> & message_listener::get_pool(){
	return pool;
}



void recv_frame_callback_c(void * request, ucs_status_t status,
							ucp_tag_recv_info_t *info) {
	try{
		std::cout<<"recv_frame_callback"<<std::endl;
		auto message_listener = ucx_message_listener::get_instance();
		message_listener->increment_frame_receiver(
			info->sender_tag & message_tag_mask); //and with message_tag_mask to set frame_id to 00 to match tag
		ucp_request_release(request);
	}
	catch(const std::exception& e)
	{
		std::cerr << "Error in recv_frame_callback_c: " << e.what() << '\n';
	}
}


void poll_for_frames(std::shared_ptr<message_receiver> receiver,
						ucp_tag_t tag, ucp_worker_h ucp_worker){
	std::cout<<"polling for frames"<<std::endl;
	while(! receiver->is_finished()){
		std::shared_ptr<ucp_tag_recv_info_t> info_tag = std::make_shared<ucp_tag_recv_info_t>();
		auto message_tag = ucp_tag_probe_nb(ucp_worker, tag, message_tag_mask, 1, info_tag.get());
		if(message_tag != NULL){
			std::cout<< "poll_for_frames: got tag"<< std::endl;
			auto converted_tag = reinterpret_cast<blazing_ucp_tag *>(&message_tag);
			auto position = converted_tag->frame_id - 1;
			receiver->allocate_buffer(position);

			auto request = ucp_tag_msg_recv_nb(ucp_worker,
																				receiver->get_buffer(position),
																				receiver->buffer_size(position),
																				ucp_dt_make_contig(1), message_tag,
																				recv_frame_callback_c);

			if(UCS_PTR_IS_ERR(request)) {
				// TODO: decide how to do cleanup i think we just throw an initialization exception
			} else if(UCS_PTR_STATUS(request) == UCS_OK) {
				std::cout<< "poll_for_frames: received"<< std::endl;
				auto message_listener = ucx_message_listener::get_instance();
				message_listener->increment_frame_receiver(tag & message_tag_mask);
			}

		}else {
			// ucp_worker_progress(ucp_worker);
			// std::cout<< "progressing poll_for_frames"<< std::endl;
			//waits until a message event occurs
            // auto status = ucp_worker_wait(ucp_worker);
            // if (status != UCS_OK){
            //   throw ::std::runtime_error("poll_for_frames: ucp_worker_wait invalid status");
            // }
		}
	}
}


void recv_begin_callback_c(void * request, ucs_status_t status,
							ucp_tag_recv_info_t *info) {

	std::cout<<"recv begin callback c"<<std::endl;
	auto message_listener = ucx_message_listener::get_instance();
	if (status != UCS_OK){
		std::cout<<"status fail in recv_begin_callback_c" <<std::endl;
		throw std::exception();
	}

	auto fwd = message_listener->get_pool().push([&message_listener, request, info](int thread_id) {
		std::cout<<"in pool of begin callback"<<std::endl;
		// auto blazing_request = reinterpret_cast<ucx_request *>(request);
		auto buffer = tag_to_begin_buffer_and_info.at(info->sender_tag).first;

		auto receiver = std::make_shared<message_receiver>(buffer);
				std::cout<<"madd receiver"<<std::endl;
		message_listener->add_receiver(info->sender_tag, receiver);
				std::cout<<"registered receiver"<<std::endl;
		//TODO: if its a specific cache get that cache adn put it here else put the general iput cache from the graph
		auto node = receiver->get_sender_node();
		std::cout<<"got node"<<std::endl;
		auto acknowledge_tag = *reinterpret_cast<blazing_ucp_tag *>(&info->sender_tag);
		acknowledge_tag.frame_id = 0xFFFF;
		auto acknowledge_tag_ucp = *reinterpret_cast<ucp_tag_t *>(&acknowledge_tag);

		auto status_acknowledge = std::make_shared<status_code>(status_code::OK);
		std::cout<<"about to send ack"<<std::endl;
		std::cout<<"ack tag is  "<<acknowledge_tag.message_id<<" "<<acknowledge_tag.worker_origin_id <<" "<<acknowledge_tag.frame_id<<std::endl;
		char * request_nbr = new char[req_size+1 + sizeof(ucx_request)];
		auto status = ucp_tag_send_nbr(
			node.get_ucp_endpoint(),
			status_acknowledge.get(),
			sizeof(status_code),
			ucp_dt_make_contig(1),
			acknowledge_tag_ucp,
			request_nbr + req_size - sizeof(ucx_request));

		std::cout<<"ucp_tag_send_nbr ACK"<<std::endl;
		if (status == UCS_INPROGRESS) {
			do {
				ucp_worker_progress(node.get_ucp_worker());
    		status = ucp_request_check_status(request_nbr + req_size - sizeof(ucx_request));
			} while (status == UCS_INPROGRESS);
		}
		if(status != UCS_OK){
			throw std::runtime_error("Was not able to send transmission ack");
		}

		std::cout<<"send ack complete"<<std::endl;

		// std::cout<<"sent "<<std::endl;
		// if(UCS_PTR_IS_ERR(request_acknowledge)) {
		// 	std::cout<<"an error occured"<<std::endl;
		// 	// TODO: decide how to do cleanup i think we just throw an initialization exception
		// } else if(UCS_PTR_STATUS(request_acknowledge) == UCS_OK) {
		// 	//nothing to do
		// 				std::cout<<"finished immediately"<<std::endl;
		// }else{
		// 				std::cout<<"callback being called"<<std::endl;
		// 	status_scope_holder[request_acknowledge] = status_acknowledge;
		// }

		poll_for_frames(receiver, info->sender_tag, message_listener->get_worker());
		// tag_to_begin_buffer_and_info.erase(info->sender_tag);
		receiver->finish();
		// message_listener->remove_receiver(info->sender_tag);
		if(request){
			ucp_request_release(request);
		}
	});
	try{
		fwd.get();
	}catch(const std::exception &e){
		std::cerr << "Error in recv_begin_callback_c: " << e.what() << std::endl;
		throw;
	}
}


void tcp_message_listener::start_polling(){
    int socket_fd, connection_fd;
    socklen_t len;
    struct sockaddr_in server_address, client_address; 

    // socket create and verification 
    socket_fd = socket(AF_INET, SOCK_STREAM, 0); 
    if (socket_fd == -1){ 
        throw std::runtime_error("Couldn't allocate socket.");
    } 

    bzero(&server_address, sizeof(server_address)); 

    server_address.sin_family = AF_INET; 
    server_address.sin_addr.s_addr = htonl(INADDR_ANY); 
    server_address.sin_port = htons(_port); 

    if (
        bind(socket_fd, (struct sockaddr*)&server_address, sizeof(server_address)) != 0
        ){ 
        throw std::runtime_error("Could not bind to socket.");
    } 

    // Now server is ready to listen and verification 
    if (listen(socket_fd, 4096) != 0) { 
        throw std::runtime_error("Could not listen on socket.");
    } 

    while((connection_fd = accept(socket_fd, (struct sockaddr *)&client_address, &len)) != -1){
        auto thread = std::thread([connection_fd, this]{
            size_t message_size;
            io::read_from_socket(connection_fd,&message_size,sizeof(message_size));

            std::vector<char> data(message_size);
            io::read_from_socket(connection_fd,data.data(),message_size);
            status_code success = status_code::OK;
            io::write_to_socket(connection_fd,&success,sizeof(success));
            
           	auto receiver = std::make_shared<message_receiver>(data); 

            auto fwd = pool.push([receiver, connection_fd](int thread_num){
                size_t buffer_size;
                size_t buffer_position = 0;
                while(buffer_position < receiver->num_buffers()){
                    receiver->allocate_buffer(buffer_position);
                    io::read_from_socket(connection_fd,receiver->get_buffer(buffer_position),receiver->buffer_size(buffer_position));
                    buffer_position++;
                }
            });
        });
        thread.detach();
    } 

}

void ucx_message_listener::poll_begin_message_tag(){
	auto thread = std::thread([this]{
		for(;;){
			// std::cout<<"starting poll begin"<<std::endl;
			std::shared_ptr<ucp_tag_recv_info_t> info_tag = std::make_shared<ucp_tag_recv_info_t>();
			auto message_tag = ucp_tag_probe_nb(
				ucp_worker, 0ull, begin_tag_mask, 1, info_tag.get());
			// std::cout<<"probed tag"<<std::endl;

			if(message_tag != NULL){
				std::cout<<"info_tag :"<< info_tag->sender_tag << std::endl;
				//we have a msg to process
				std::cout<<"poll_begin_message_tag: message found!"<<std::endl;
				tag_to_begin_buffer_and_info[info_tag->sender_tag] = std::make_pair(
					std::vector<char>(info_tag->length), info_tag);
				auto request = ucp_tag_msg_recv_nb(ucp_worker,
					tag_to_begin_buffer_and_info[info_tag->sender_tag].first.data(),
					info_tag->length,
					ucp_dt_make_contig(1), message_tag,
					recv_begin_callback_c);



				if(UCS_PTR_IS_ERR(request)) {
					// TODO: decide how to do cleanup i think we just throw an initialization exception
				} else if(UCS_PTR_STATUS(request) == UCS_OK) {
					std::cout<< "poll_begin_message_tag RECV immediate"<<std::endl;
					recv_begin_callback_c(request, UCS_OK, info_tag.get());
				}

				std::cout<<">>>>>>>>>   probed tag SUCCESS GONNA BREAK"<<std::endl;
				
			}else {
				// std::cout<<"no messages"<<std::endl;
			  // ucp_worker_progress(ucp_worker);
				// std::this_thread::sleep_for(2s);
				// waits until a message event occurs
				// auto status = ucp_worker_wait(ucp_worker);
				// if (status != UCS_OK){
				// 	throw ::std::runtime_error("poll_begin_message_tag: ucp_worker_wait invalid status");
				// }

			}
    }

		std::cout<<">>>>>>>>>   FINISHED poll_begin_message_tag"<<std::endl;
	});
	thread.detach();
}


void ucx_message_listener::add_receiver(ucp_tag_t tag,std::shared_ptr<message_receiver> receiver){
	tag_to_receiver[tag] = receiver;
}
void ucx_message_listener::remove_receiver(ucp_tag_t tag){
	tag_to_receiver.erase(tag);
}

ucp_worker_h ucx_message_listener::get_worker(){
	return ucp_worker;
}

void ucx_message_listener::increment_frame_receiver(ucp_tag_t tag){
	tag_to_receiver[tag]->confirm_transmission();
}
ucx_message_listener * ucx_message_listener::instance = nullptr;

ucx_message_listener::ucx_message_listener(ucp_worker_h worker, int num_threads) : ucp_worker{ucp_worker}, message_listener(num_threads) {

}


void ucx_message_listener::initialize_message_listener(ucp_worker_h worker, int num_threads){
	if(instance == NULL) {
		instance = new ucx_message_listener(worker,num_threads);
	}
}

void ucx_message_listener::start_polling(){
   poll_begin_message_tag(); 
}

ucx_message_listener * ucx_message_listener::get_instance() {
	if(instance == NULL) {
		throw::std::exception();
	}
	return instance;
}

}//namespace comm