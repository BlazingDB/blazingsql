
#include <map>
#include <vector>

#include "protocols.hpp"
#include "messageReceiver.hpp"

#include <ucp/api/ucp.h>
#include <ucp/api/ucp_def.h>
#include <thread>


#include "execution_graph/logic_controllers/CacheMachine.h"
#include "transport/io/reader_writer.h"

constexpr size_t NUMBER_RETRIES = 20;
constexpr size_t FILE_RETRY_DELAY = 20;


namespace io{


	void read_from_socket(int socket_fd, void * data, size_t read_size){
		size_t amount_read = 0;
		int bytes_read = 0;
        size_t count_invalids = 0;
		while (amount_read < read_size && count_invalids < NUMBER_RETRIES) {
			bytes_read = read(socket_fd, static_cast<uint8_t*>(data) + amount_read, read_size - amount_read);
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

    void write_to_socket(int socket_fd, const void * data, size_t write_size){
		size_t amount_written = 0;
		int bytes_written = 0;
        size_t count_invalids = 0;
		while (amount_written < write_size && count_invalids < NUMBER_RETRIES) {
			bytes_written = write(socket_fd, data + amount_written, write_size - amount_written);

			if (bytes_written != -1) {
				amount_written += bytes_written;
				count_invalids = 0;
			} else {
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

ucp_progress_manager * instance = nullptr;
ucp_progress_manager * ucp_progress_manager::get_instance(ucp_worker_h ucp_worker, size_t request_size) {
	if(instance == nullptr){
        instance = new ucp_progress_manager(ucp_worker,request_size);
    }
	return instance;
}

ucp_progress_manager * ucp_progress_manager::get_instance() {
	if(instance == nullptr){
        throw std::runtime_error("ucp_progress_manager (in blazing) not initialized.");
    }
	return instance;
}

ucp_progress_manager::ucp_progress_manager(ucp_worker_h ucp_worker, size_t request_size) :
 _request_size{request_size}, ucp_worker{ucp_worker} {
    std::thread t([this]{
        cudaSetDevice(0);
        this->check_progress();
    });
    t.detach();
}

void ucp_progress_manager::add_recv_request(char * request, std::function<void()> callback){
    {
        std::lock_guard<std::mutex> lock(request_mutex);
        recv_requests.insert({request, callback});
    }
    cv.notify_all();
}


void ucp_progress_manager::add_send_request(char * request, std::function<void()> callback){
    {
        std::lock_guard<std::mutex> lock(request_mutex);
        send_requests.insert({request, callback});
    }
    cv.notify_all();
}

void ucp_progress_manager::check_progress(){
    while(true){
        std::set<request_struct> cur_send_requests;
        std::set<request_struct> cur_recv_requests;
        {
            std::unique_lock<std::mutex> lock(request_mutex);
            cv.wait(lock,[this]{
                return (send_requests.size() + recv_requests.size()) > 0;
            });
            cur_send_requests = send_requests;
            cur_recv_requests = recv_requests;
        }

    	ucp_worker_progress(ucp_worker);

        for(const auto & req_struct : cur_send_requests){
            auto status = ucp_request_check_status(req_struct.request + _request_size);
            // std::cout<<"checked status of "<<(void *) req_struct.request<<" it was "<<status <<std::endl;
            if (status == UCS_OK){
                {
                    std::lock_guard<std::mutex> lock(request_mutex);
                    this->send_requests.erase(req_struct);
                }
                delete req_struct.request;
                req_struct.callback();
            } else if (status != UCS_INPROGRESS){
                throw std::runtime_error("Communication error in check_progress for send_requests.");
            }
        }

        for(const auto & req_struct : cur_recv_requests){
            auto status = ucp_request_check_status(req_struct.request + _request_size);
            // std::cout<<"checked status of "<<(void *) req_struct.request<<" it was "<<status <<std::endl;
            if (status == UCS_OK){
                {
                    std::lock_guard<std::mutex> lock(request_mutex);
                    this->recv_requests.erase(req_struct);
                }
                delete req_struct.request;
                req_struct.callback();
            }else if (status != UCS_INPROGRESS){
                throw std::runtime_error("Communication error in check_progress for recv_requests.");
            }
        }

        // std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
}



void graphs_info::register_graph(int32_t ctx_token, std::shared_ptr<ral::cache::graph> graph){
	_ctx_token_to_graph_map.insert({ctx_token, graph});
}

void graphs_info::deregister_graph(int32_t ctx_token){
	if(_ctx_token_to_graph_map.find(ctx_token) != _ctx_token_to_graph_map.end()){
        _ctx_token_to_graph_map[ctx_token]->clear_kernels();
		_ctx_token_to_graph_map.erase(ctx_token);
	} else{
       // std::cout<<"did not clear kernels "<<ctx_token<<std::endl;
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
    uint16_t ral_id)
    : buffer_transport(metadata, buffer_sizes, column_transports,destinations),
    origin_node(origin_node), ral_id{ral_id}, _request_size{request_size} {
        tag = generate_message_tag();
}

ucx_buffer_transport::~ucx_buffer_transport() {
}

std::atomic<int> atomic_message_id(0);

ucp_tag_t ucx_buffer_transport::generate_message_tag() {
    auto current_message_id = atomic_message_id.fetch_add(1);
    blazing_ucp_tag blazing_tag = {current_message_id, ral_id, 0U};
    this->message_id = blazing_tag.message_id;
    return *reinterpret_cast<ucp_tag_t *>(&blazing_tag);
}

void ucx_buffer_transport::send_begin_transmission() {
    std::shared_ptr<std::vector<char>> buffer_to_send = std::make_shared<std::vector<char>>(detail::serialize_metadata_and_transports_and_buffer_sizes(metadata, column_transports, buffer_sizes));

    std::vector<char *> requests(destinations.size());
    int i = 0;
    for(auto const & node : destinations) {
        char * request = new char[_request_size];
        //auto temp_tag = *reinterpret_cast<blazing_ucp_tag *>(&tag);
        auto status = ucp_tag_send_nbr(
            node.get_ucp_endpoint(), buffer_to_send->data(), buffer_to_send->size(), ucp_dt_make_contig(1), tag, request + _request_size);

        if (!UCS_STATUS_IS_ERR(status)) {
            ucp_progress_manager::get_instance()->add_send_request(request, [buffer_to_send, this]() mutable {
                buffer_to_send.reset();
                this->increment_begin_transmission();
            });
        }else {
            throw std::runtime_error("Immediate Communication error in send_begin_transmission.");
        }

		i++;
	}
    reinterpret_cast<blazing_ucp_tag *>(&tag)->frame_id++;
}

void ucx_buffer_transport::send_impl(const char * buffer, size_t buffer_size) {
  blazing_ucp_tag blazing_tag = *reinterpret_cast<blazing_ucp_tag *>(&tag);
  for (auto const &node : destinations) {
    char *request = new char[_request_size];
    auto status = ucp_tag_send_nbr(node.get_ucp_endpoint(),
                                    buffer,
                                    buffer_size,
                                    ucp_dt_make_contig(1),
                                    *reinterpret_cast<ucp_tag_t *>(&blazing_tag),
                                    request + _request_size);

    if (!UCS_STATUS_IS_ERR(status)) {
        ucp_progress_manager::get_instance()->add_send_request(request, [this](){ this->increment_frame_transmission(); });
    } else {
        throw std::runtime_error("Immediate Communication error in send_impl.");
    }
  }
  reinterpret_cast<blazing_ucp_tag *>(&tag)->frame_id++;
}

tcp_buffer_transport::tcp_buffer_transport(
        std::vector<node> destinations,
        ral::cache::MetadataDictionary metadata,
        std::vector<size_t> buffer_sizes,
        std::vector<blazingdb::transport::ColumnTransport> column_transports,
        uint16_t ral_id,
        ctpl::thread_pool<BlazingThread> * allocate_copy_buffer_pool)
        : buffer_transport(metadata, buffer_sizes, column_transports,destinations),
        ral_id{ral_id}, allocate_copy_buffer_pool{allocate_copy_buffer_pool} {

        //Initialize connection to get
    cudaStreamCreate(&stream);
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
    std::vector<char> buffer_to_send = detail::serialize_metadata_and_transports_and_buffer_sizes(metadata, column_transports,buffer_sizes);
	auto size_to_send = buffer_to_send.size();

    for (auto socket_fd : socket_fds){
        //write out begin_message_size
        io::write_to_socket(socket_fd, &size_to_send ,sizeof(size_to_send));
        io::write_to_socket(socket_fd, buffer_to_send.data(),buffer_to_send.size());
        //io::read_from_socket(socket_fd,&status, sizeof(status_code));
        //if(status != status_code::OK){
         //   throw std::runtime_error("Could not send begin transmission");
        //}
        increment_begin_transmission();
    }

}

void tcp_buffer_transport::send_impl(const char * buffer, size_t buffer_size){
    try{
        for (auto socket_fd : socket_fds){
            io::write_to_socket(socket_fd, buffer,buffer_size);
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
    cudaStreamDestroy(stream);
}

}  // namespace comm
