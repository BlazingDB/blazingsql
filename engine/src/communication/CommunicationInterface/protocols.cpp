
#include <map>
#include <vector>

#include "protocols.hpp"
#include "messageReceiver.hpp"

#include <ucp/api/ucp.h>
#include <ucp/api/ucp_def.h>
#include <thread>


#include "execution_graph/logic_controllers/CacheMachine.h"
#include "blazingdb/transport/io/reader_writer.h"

constexpr size_t NUMBER_RETRIES = 20;
constexpr size_t FILE_RETRY_DELAY = 20;


namespace io{


	void read_from_socket(int socket_fd, void * data, size_t read_size){
		size_t amount_read = 0;
		int bytes_read = 0;
		int count_invalids = 0;
		while (amount_read < read_size && count_invalids < NUMBER_RETRIES) {
			bytes_read = read(socket_fd, data + amount_read, read_size - amount_read);
			if (bytes_read != -1) {
				amount_read += bytes_read;
				count_invalids = 0;
			} else {
				if (errno == 9) { // Bad socket number
					std::cerr << "Bad socket reading from  " << socket_fd << std::endl;
					throw std::runtime_error("Bad socket");
				}
				const int sleep_milliseconds = (count_invalids + 1) * FILE_RETRY_DELAY;
				std::this_thread::sleep_for(std::chrono::milliseconds(sleep_milliseconds));
				count_invalids++;
			}
		}
    }

    void write_to_socket(int socket_fd, void * data, size_t write_size){
		size_t amount_written = 0;
		int bytes_written = 0;
		int count_invalids = 0;
		while (amount_written < write_size && count_invalids < NUMBER_RETRIES) {
			bytes_written = write(socket_fd, data + amount_written, write_size - amount_written);
            std::cout<<"wrote "<<bytes_written<<" bytes of"<<write_size<<std::endl;
			if (bytes_written != -1) {
				amount_written += bytes_written;
				count_invalids = 0;
			} else {
                std::cout<<errno<<" is errno"<<std::endl;
				if (errno == 9) { // Bad socket number
					std::cerr << "Bad socket writing to " << socket_fd << std::endl;
					throw std::runtime_error("Bad socket");
				}
				// TODO: add check to make sure that the task was not handlerThread
				const int sleep_milliseconds = (count_invalids + 1) * FILE_RETRY_DELAY;
				std::this_thread::sleep_for(
					std::chrono::milliseconds(sleep_milliseconds));
				if (count_invalids < 300) {
					count_invalids++;
				}
			}
		}
    }
}


namespace comm {

graphs_info & graphs_info::getInstance() {
	static graphs_info instance;
	return instance;
}

void graphs_info::register_graph(int32_t ctx_token, std::shared_ptr<ral::cache::graph> graph){
	_ctx_token_to_graph_map.insert({ctx_token, graph});
}

void graphs_info::deregister_graph(int32_t ctx_token){
	if(_ctx_token_to_graph_map.find(ctx_token) != _ctx_token_to_graph_map.end()){
		_ctx_token_to_graph_map.erase(ctx_token);
	}
}
std::shared_ptr<ral::cache::graph> graphs_info::get_graph(int32_t ctx_token) {
	if(_ctx_token_to_graph_map.find(ctx_token) == _ctx_token_to_graph_map.end()){
		throw std::runtime_error("Graph not found");
	}
    return _ctx_token_to_graph_map.at(ctx_token);
}


ucx_buffer_transport::ucx_buffer_transport(size_t request_size,
	ucp_worker_h origin_node,
    std::vector<node> destinations,
	ral::cache::MetadataDictionary metadata,
	std::vector<size_t> buffer_sizes,
	std::vector<blazingdb::transport::ColumnTransport> column_transports,
	int ral_id)
	: ral_id{ral_id}, buffer_transport(metadata, buffer_sizes, column_transports,destinations), origin_node(origin_node), _request_size{request_size}{
	tag = generate_message_tag();
}

ucx_buffer_transport::~ucx_buffer_transport() {
}

std::atomic<int> atomic_message_id(0);

ucp_tag_t ucx_buffer_transport::generate_message_tag() {
	auto current_message_id = atomic_message_id.fetch_add(1);
	blazing_ucp_tag blazing_tag = {current_message_id, ral_id, 0u};
	this->message_id = blazing_tag.message_id;
	return *reinterpret_cast<ucp_tag_t *>(&blazing_tag);
}

void ucx_buffer_transport::send_begin_transmission() {
	std::vector<char> buffer_to_send = detail::serialize_metadata_and_transports_and_buffer_sizes(metadata, column_transports, buffer_sizes);

	std::vector<char *> requests(destinations.size());
	int i = 0;
	for(auto const & node : destinations) {
        requests[i] = new char[_request_size];
		auto temp_tag = *reinterpret_cast<blazing_ucp_tag *>(&tag);
		auto status = ucp_tag_send_nbr(
			node.get_ucp_endpoint(), buffer_to_send.data(), buffer_to_send.size(), ucp_dt_make_contig(1), tag, requests[i] + _request_size);
        do {
			ucp_worker_progress(origin_node);
			status = ucp_request_check_status(requests[i] + _request_size);
		} while (status == UCS_INPROGRESS);
        if(status != UCS_OK){
			throw std::runtime_error("Was not able to send begin transmission to " + node.id());
		}

		ucp_tag_recv_info_t info_tag;
		blazing_ucp_tag acknowledge_tag = *reinterpret_cast<blazing_ucp_tag *>(&tag);
		acknowledge_tag.frame_id = 0xFFFF;

		requests[i] = new char[_request_size];

		status_code recv_begin_status = status_code::INVALID;
		status = ucp_tag_recv_nbr(origin_node,
									&recv_begin_status,
									sizeof(status_code),
									ucp_dt_make_contig(1),
									*reinterpret_cast<ucp_tag_t *>(&acknowledge_tag),
									acknownledge_tag_mask,
									requests[i] + _request_size);
        do {
			ucp_worker_progress(origin_node);
			ucp_tag_recv_info_t info_tag;
			status = ucp_tag_recv_request_test(requests[i] + _request_size, &info_tag);
		} while (status == UCS_INPROGRESS);
        if(status != UCS_OK){
			throw std::runtime_error("Was not able to receive acknowledgment of begin transmission from " + node.id());
		}

		if (recv_begin_status == status_code::OK) {
			increment_begin_transmission();
		}

		i++;
	}
}

void ucx_buffer_transport::increment_frame_transmission() {
	buffer_transport::increment_frame_transmission();
	reinterpret_cast<blazing_ucp_tag *>(&tag)->frame_id++;
}

void ucx_buffer_transport::increment_begin_transmission(){
	buffer_transport::increment_begin_transmission();
	reinterpret_cast<blazing_ucp_tag *>(&tag)->frame_id++;
}

void ucx_buffer_transport::send_impl(const char * buffer, size_t buffer_size) {
	blazing_ucp_tag blazing_tag = *reinterpret_cast<blazing_ucp_tag *>(&tag);
  std::vector<char *> requests;
  requests.reserve(destinations.size());
  for (auto const &node : destinations) {
    char *request = reinterpret_cast<char *>(std::malloc(_request_size));
    ucp_tag_send_nbr(node.get_ucp_endpoint(),
                     buffer,
                     buffer_size,
                     ucp_dt_make_contig(1),
                     *reinterpret_cast<ucp_tag_t *>(&blazing_tag),
                     request + _request_size);
    requests.push_back(request);
  }

  // TODO: add a max attempts
  std::vector<ucs_status_t> statuses;
  statuses.resize(requests.size());
  do {
    ucp_worker_progress(origin_node);
    std::transform(requests.cbegin(),
                   requests.cend(),
                   statuses.begin(),
                   [this](char *request) {
                     return ucp_request_check_status(request + _request_size);
                   });
  } while (std::find(statuses.cbegin(), statuses.cend(), UCS_INPROGRESS) !=
           statuses.cend());

  for (std::size_t i = 0; i < requests.size(); i++) {
    char *request = requests.at(i);
    ucs_status_t status = statuses.at(i);

    if (status == UCS_OK) {
      std::free(request);
      increment_frame_transmission();
    } else {
			std::cout<<"Something went wrong while sending frame data"<<std::endl;
    }
  }
}

tcp_buffer_transport::tcp_buffer_transport(
        std::vector<node> destinations,
		ral::cache::MetadataDictionary metadata,
		std::vector<size_t> buffer_sizes,
		std::vector<blazingdb::transport::ColumnTransport> column_transports,
        int ral_id,
        ctpl::thread_pool<BlazingThread> * allocate_copy_buffer_pool)
        : ral_id{ral_id}, buffer_transport(metadata, buffer_sizes, column_transports,destinations),
	    allocate_copy_buffer_pool{allocate_copy_buffer_pool} {

        //Initialize connection to get

    for(auto destination : destinations){
        int socket_fd;
        struct sockaddr_in address;

        if ((socket_fd = socket(AF_INET, SOCK_STREAM, 0)) < 0)
        {
            throw std::runtime_error("Could not open communication socket");
        }
        address.sin_family = AF_INET;
        address.sin_port = htons(destination.port());

        if(inet_pton(AF_INET, destination.ip().c_str(), &address.sin_addr)<=0)
        {
            throw std::runtime_error("Invalid Communication Address");
        }
        if (connect(socket_fd, (struct sockaddr *)&address, sizeof(address)) < 0)
        {
            throw std::runtime_error("Invalid Communication Address");
        }
        socket_fds.push_back(socket_fd);
    }
}

void tcp_buffer_transport::send_begin_transmission(){
    status_code status;
    std::vector<char> buffer_to_send = detail::serialize_metadata_and_transports_and_buffer_sizes(metadata, column_transports,buffer_sizes);
	auto size_to_send = buffer_to_send.size();

    for (auto socket_fd : socket_fds){
        //write out begin_message_size
        io::write_to_socket(socket_fd, &size_to_send ,sizeof(size_to_send));
        io::write_to_socket(socket_fd, buffer_to_send.data(),buffer_to_send.size());
        io::read_from_socket(socket_fd,&status, sizeof(status_code));
        if(status != status_code::OK){
            throw std::runtime_error("Could not send begin transmission");
        }
        increment_begin_transmission();
    }

}

void tcp_buffer_transport::send_impl(const char * buffer, size_t buffer_size){
    //this is where it gets fuzzy...

    //allocate pinned + copy from gpu
    //transmit
    std::cout<<"sending data "<<1<<std::endl;
    size_t pinned_buffer_size = blazingdb::transport::io::getPinnedBufferProvider().sizeBuffers();
    size_t num_chunks = (buffer_size +(pinned_buffer_size - 1))/ pinned_buffer_size;
    std::cout<<"sending data "<<2<<std::endl;
    std::vector<std::future<blazingdb::transport::io::PinnedBuffer *> > buffers;
    for( size_t chunk = 0; chunk < num_chunks; chunk++ ){
        
        size_t chunk_size = pinned_buffer_size;
        if(( chunk + 1) == num_chunks){ // if its the last chunk, we chunk_size is different
            chunk_size = buffer_size - (chunk * pinned_buffer_size);
        }
        auto buffer_chunk_start = buffer + (chunk * pinned_buffer_size);
        
            std::cout<<"sending data "<<4<<std::endl;
        buffers.push_back(
            std::move(allocate_copy_buffer_pool->push(
                [buffer_chunk,chunk_size](int thread_id) {
                    auto pinned_buffer = blazingdb::transport::io::getPinnedBufferProvider().getBuffer();
                    pinned_buffer->use_size = chunk_size;
                    cudaMemcpyAsync(pinned_buffer->data,buffer_chunk_start,chunk_size,cudaMemcpyDeviceToHost,pinned_buffer->stream);
                    return pinned_buffer;
            }))
        );
            std::cout<<"sending data "<<5<<std::endl;
    }
    size_t chunk = 0;
    try{
            std::cout<<"sending data "<<6<<std::endl;
        while(chunk < num_chunks){
    std::cout<<"sending data "<<7<<std::endl;
            auto pinned_buffer = buffers[chunk].get();
            std::cout<<"sending data "<<7.5<<std::endl;
            cudaStreamSynchronize(pinned_buffer->stream);
            for (auto socket_fd : socket_fds){
               // io::write_to_socket(socket_fd, &pinned_buffer->use_size,sizeof(pinned_buffer->use_size));
                io::write_to_socket(socket_fd, pinned_buffer->data,pinned_buffer->use_size);
            }
                std::cout<<"sending data "<<8<<std::endl;
            blazingdb::transport::io::getPinnedBufferProvider().freeBuffer(pinned_buffer);
            chunk++;
        }

        for (auto socket_fd : socket_fds){
            increment_frame_transmission();
        }
    }catch(const std::exception & e ){
        throw;
    }

}

tcp_buffer_transport::~tcp_buffer_transport(){
    for (auto socket_fd : socket_fds){
        close(socket_fd);
    }
}

}  // namespace comm
