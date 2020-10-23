#include "messageListener.hpp"
#include <sys/socket.h>

#include "blazingdb/transport/io/reader_writer.h"
#include "CodeTimer.h"

namespace comm {

std::map<ucp_tag_t, std::pair<std::vector<char>, std::shared_ptr<ucp_tag_recv_info_t> > > tag_to_begin_buffer_and_info;


ctpl::thread_pool<BlazingThread> & message_listener::get_pool(){
	return pool;
}

std::map<std::string, comm::node> message_listener::get_node_map(){
	return _nodes_info_map;
}



void recv_frame_callback_c(void * request, ucs_status_t status,
							ucp_tag_recv_info_t *info) {
	try{
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
		                 ucp_tag_t tag,
                     ucp_worker_h ucp_worker,
                     const std::size_t request_size){
	blazing_ucp_tag message_tag = *reinterpret_cast<blazing_ucp_tag *>(&tag);
	int buffer_id = 0;
  while (!receiver->is_finished()) {
    receiver->allocate_buffer(buffer_id);

		message_tag.frame_id = buffer_id + 1;

    char *request = new char[request_size];
    ucs_status_t status = ucp_tag_recv_nbr(ucp_worker,
                                           receiver->get_buffer(buffer_id),
                                           receiver->buffer_size(buffer_id),
                                           ucp_dt_make_contig(1),
                                           *reinterpret_cast<ucp_tag_t *>(&message_tag),
                                           message_tag_mask,
                                           request + request_size);

    do {
      ucp_worker_progress(ucp_worker);
      status = ucp_request_check_status(request + request_size);
    } while (status == UCS_INPROGRESS);

    if (status == UCS_OK) {
      auto message_listener = ucx_message_listener::get_instance();
      message_listener->increment_frame_receiver(tag & message_tag_mask);
    } else {
      // TODO: decide how to do cleanup i think we just throw an
      // initialization exception
    }
    delete request;

		++buffer_id;
  }
  receiver->finish();
}


void recv_begin_callback_c(ucp_tag_recv_info_t *info, size_t request_size) {

	auto message_listener = ucx_message_listener::get_instance();

	auto fwd = message_listener->get_pool().push([&message_listener, info, request_size](int thread_id) {
		auto buffer = tag_to_begin_buffer_and_info.at(info->sender_tag).first;

		auto receiver = std::make_shared<message_receiver>(message_listener->get_node_map(), buffer);
		message_listener->add_receiver(info->sender_tag, receiver);
		//TODO: if its a specific cache get that cache adn put it here else put the general iput cache from the graph
/*
		auto node = receiver->get_sender_node();

		auto acknowledge_tag = *reinterpret_cast<blazing_ucp_tag *>(&info->sender_tag);
		acknowledge_tag.frame_id = 0xFFFF;
		auto acknowledge_tag_ucp = *reinterpret_cast<ucp_tag_t *>(&acknowledge_tag);

		auto status_acknowledge = std::make_shared<status_code>(status_code::OK);
		char * request_nbr = new char[request_size];
		auto status = ucp_tag_send_nbr(
			node.get_ucp_endpoint(),
			status_acknowledge.get(),
			sizeof(status_code),
			ucp_dt_make_contig(1),
			acknowledge_tag_ucp,
			request_nbr + request_size);

		do {
			ucp_worker_progress(node.get_ucp_worker());
			status = ucp_request_check_status(request_nbr + request_size);
		} while (status == UCS_INPROGRESS);

		if(status != UCS_OK){
			throw std::runtime_error("Was not able to send transmission ack");
		}
*/
		poll_for_frames(receiver, info->sender_tag, message_listener->get_worker(), request_size);
	});
	try{
		fwd.get();
	}catch(const std::exception &e){
		std::cerr << "Error in recv_begin_callback_c: " << e.what() << std::endl;
		throw;
	}
}


void tcp_message_listener::start_polling(){
    

	if (!polling_started){
		int socket_fd;
		
		struct sockaddr_in server_address;

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
    auto thread = std::thread([this, socket_fd] {
      struct sockaddr_in client_address;
      socklen_t len;
      int connection_fd;
      // TODO: be able to stop this thread from running when the engine is killed
      while((connection_fd = accept(socket_fd, (struct sockaddr *) &client_address, &len)) != -1) {
        pool.push([this, connection_fd](int thread_num) {
          CodeTimer timer;
          cudaStream_t stream = 0;
//          cudaStreamCreate(&stream);
          size_t message_size;
          io::read_from_socket(connection_fd, &message_size, sizeof(message_size));

          std::vector<char> data(message_size);
          io::read_from_socket(connection_fd, data.data(), message_size);
          auto meta_read_time = timer.elapsed_time();
          //status_code success = status_code::OK;
          //io::write_to_socket(connection_fd, &success, sizeof(success));
		{
          auto receiver = std::make_shared<message_receiver>(_nodes_info_map, data);

        //	std::cout<<"elapsed time make receiver done "<<timer.elapsed_time()<<std::endl;
          auto receiver_time = timer.elapsed_time() - meta_read_time;
          size_t pinned_buffer_size = blazingdb::transport::io::getPinnedBufferProvider().sizeBuffers();
          size_t buffer_position = 0;
          size_t total_size = 0;
          size_t total_allocate_time = 0 ;
          size_t total_read_time = 0 ;
          size_t total_sync_time = 0;
          while(buffer_position < receiver->num_buffers()) {
            size_t buffer_size = receiver->buffer_size(buffer_position);
            total_size += buffer_size;
            size_t num_chunks = (buffer_size +(pinned_buffer_size - 1))/ pinned_buffer_size;
            std::vector<blazingdb::transport::io::PinnedBuffer*> pinned_buffers(num_chunks);
            CodeTimer timer_2;
            receiver->allocate_buffer(buffer_position,stream);
            void * buffer = receiver->get_buffer(buffer_position);
            auto prev_timer_2 = timer_2.elapsed_time();
            total_allocate_time += timer_2.elapsed_time();
            timer_2.reset();
            for( size_t chunk = 0; chunk < num_chunks; chunk++ ){
              size_t chunk_size = pinned_buffer_size;
              if(( chunk + 1) == num_chunks){ // if its the last chunk, we chunk_size is different
                chunk_size = buffer_size - (chunk * pinned_buffer_size);
              }
              auto pinned_buffer = blazingdb::transport::io::getPinnedBufferProvider().getBuffer();
              pinned_buffer->use_size = chunk_size;

              io::read_from_socket(connection_fd, pinned_buffer->data, chunk_size);

              auto buffer_chunk_start = buffer + (chunk * pinned_buffer_size);
              cudaMemcpyAsync(buffer_chunk_start, pinned_buffer->data, chunk_size, cudaMemcpyHostToDevice, stream);
              pinned_buffers[chunk] = pinned_buffer;
            }

            //std::cout<<"elapsed copy from gpu before synch "<<timer_2.elapsed_time()<<std::endl;
            total_read_time += timer_2.elapsed_time();
            timer_2.reset();
            // TODO: Do we want to do this synchronize and free after all the receiver->num_buffers() or for each one?
            cudaStreamSynchronize(stream);

            total_sync_time += timer_2.elapsed_time();
            //std::cout<<"elapsed copy from gpu after synch "<<timer_2.elapsed_time()<<std::endl;

            for( size_t chunk = 0; chunk < num_chunks; chunk++ ){
              blazingdb::transport::io::getPinnedBufferProvider().freeBuffer(pinned_buffers[chunk]);
            }
            buffer_position++;
          }
		close(connection_fd);
          auto duration = timer.elapsed_time();
          std::cout<<"Transfer duration before finish "<<duration <<" Throughput was "<<
          (( (float) total_size) / 1000000.0)/(((float) duration)/1000.0)<<" MB/s"<<std::endl;

          receiver->finish(stream);
		auto duration_2 = timer.elapsed_time();
		std::cout<<"Transfer duration with finish "<<duration <<" Throughput was "<<
		(( (float) total_size) / 1000000.0)/(((float) duration)/1000.0)<<" MB/s"<<std::endl;
		
		std::cout<<"META, Recevier, allocate, read, synchronize, total_before_finish, total_after_finish,bytes\n"<<
		meta_read_time<<", "<<receiver_time<<", "<<total_allocate_time<<", "<<total_read_time<<","<<total_sync_time<<", "<<duration<<","<<duration_2<<", "<<total_size<<std::endl;

		}
		cudaStreamSynchronize(stream);
	//	cudaStreamDestroy(stream);
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
			// cudaSetDevice(0);

			for(;;){
				std::shared_ptr<ucp_tag_recv_info_t> info_tag = std::make_shared<ucp_tag_recv_info_t>();
				ucp_tag_message_h message_tag = nullptr;
				do {
					message_tag = ucp_tag_probe_nb(
						ucp_worker, 0ull, begin_tag_mask, 0, info_tag.get());

					// NOTE: comment this out when running using dask workers, it crashes for some reason
					if (running_from_unit_test && message_tag == nullptr) {
						ucp_worker_progress(ucp_worker);
					}
				}while(message_tag == nullptr);

					char * request = new char[_request_size];

					//we have a msg to process
					tag_to_begin_buffer_and_info[info_tag->sender_tag] = std::make_pair(
						std::vector<char>(info_tag->length), info_tag);

					auto status = ucp_tag_recv_nbr(ucp_worker,
						tag_to_begin_buffer_and_info[info_tag->sender_tag].first.data(),
						info_tag->length,
						ucp_dt_make_contig(1),
						0ull,
						begin_tag_mask,
						request + _request_size);

					do {
						ucp_worker_progress(ucp_worker);
						ucp_tag_recv_info_t info_tag_;
						status = ucp_tag_recv_request_test(request + _request_size, &info_tag_);
					} while (status == UCS_INPROGRESS);

					if(status != UCS_OK){
						throw std::runtime_error("Was not able to receive begin message");
					}

					recv_begin_callback_c( info_tag.get(), _request_size);
		}

		});
		thread.detach();
	}
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
tcp_message_listener * tcp_message_listener::instance = nullptr;

ucx_message_listener::ucx_message_listener(ucp_context_h context, ucp_worker_h worker, const std::map<std::string, comm::node>& nodes, int num_threads) :
	message_listener(nodes, num_threads), ucp_worker{worker}
{
  ucp_context_attr_t attr;
  attr.field_mask = UCP_ATTR_FIELD_REQUEST_SIZE;
  ucs_status_t status = ucp_context_query(context, &attr);
	if (status != UCS_OK)	{
		throw std::runtime_error("Error calling ucp_context_query");
	}
	_request_size = attr.request_size;
	ucp_progress_manager::get_instance(worker,_request_size);
}

tcp_message_listener::tcp_message_listener(const std::map<std::string, comm::node>& nodes,int port, int num_threads) : _port{port} , message_listener{nodes,num_threads}{

}

void ucx_message_listener::initialize_message_listener(ucp_context_h context, ucp_worker_h worker, const std::map<std::string, comm::node>& nodes, int num_threads){
	if(instance == NULL) {
		instance = new ucx_message_listener(context, worker, nodes, num_threads);
	}
}

void tcp_message_listener::initialize_message_listener(const std::map<std::string, comm::node>& nodes, int port, int num_threads){
	if(instance == NULL){
		instance = new tcp_message_listener(nodes,port,num_threads);
	}
}

void ucx_message_listener::start_polling(){
	if (!polling_started){
   		poll_begin_message_tag(false);
	}
}

ucx_message_listener * ucx_message_listener::get_instance() {
	if(instance == NULL) {
		throw::std::exception();
	}
	return instance;
}

tcp_message_listener * tcp_message_listener::get_instance() {
	if(instance == NULL) {
		throw::std::exception();
	}
	return instance;
}

}//namespace comm
