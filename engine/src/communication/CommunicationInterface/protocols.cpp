
#include <map>
#include <vector>

#include "protocols.hpp"
#include "messageReceiver.hpp"

#include <ucp/api/ucp.h>
#include <ucp/api/ucp_def.h>
#include <thread>


#include "execution_graph/logic_controllers/CacheMachine.h"

namespace comm {

ucp_nodes_info & ucp_nodes_info::getInstance() {
	static ucp_nodes_info instance;
	return instance;
}

void ucp_nodes_info::init(const std::map<std::string, node> & nodes_map) {
	_id_to_node_info_map = nodes_map;
}

node ucp_nodes_info::get_node(const std::string& id) { return _id_to_node_info_map.at(id); }


graphs_info & graphs_info::getInstance() {
	static graphs_info instance;
	return instance;
}

void graphs_info::register_graph(int32_t ctx_token, std::shared_ptr<ral::cache::graph> graph){
	_ctx_token_to_graph_map.insert({ctx_token, graph});
}

void graphs_info::deregister_graph(int32_t ctx_token){
	std::cout<<"erasing from map"<<std::endl;
	if(_ctx_token_to_graph_map.find(ctx_token) == _ctx_token_to_graph_map.end()){
		std::cout<<"token not found!"<<std::endl;
	}else{
		std::cout<<"erasing token"<<std::endl;
			_ctx_token_to_graph_map.erase(ctx_token);

	}
}
std::shared_ptr<ral::cache::graph> graphs_info::get_graph(int32_t ctx_token) {
	if(_ctx_token_to_graph_map.find(ctx_token) == _ctx_token_to_graph_map.end()){
		std::cout<<"Graph with token"<<ctx_token<<" is not found!"<<std::endl;
		throw std::runtime_error("Graph not found");
	}
	return _ctx_token_to_graph_map.at(ctx_token); }


std::map<ucp_tag_t, std::pair<std::vector<char>, std::shared_ptr<ucp_tag_recv_info_t> > > tag_to_begin_buffer_and_info;

/**
 * A struct that lets us access the request that the end points ucx-py generates.
 */
struct ucx_request {
	//!!!!!!!!!!! do not modify this struct this has to match what is found in
	// https://github.com/rapidsai/ucx-py/blob/branch-0.15/ucp/_libs/ucx_api.pyx
	// Make sure to check on the latest branch !!!!!!!!!!!!!!!!!!!!!!!!!!!
	int completed; /**< Completion flag that we do not use. */
	int uid;	   /**< We store a map of request uid ==> buffer_transport to manage completion of send */
};

static size_t req_size = 256 + sizeof(ucx_request);

enum class status_code {
	INVALID = -1,
	OK = 1,
	ERROR = 0
};

// TODO: remove this hack when we can modify the ucx_request
// object so we cna send in our c++ callback via the request
std::map<int, ucx_buffer_transport *> message_uid_to_buffer_transport;

std::map<ucp_tag_t, std::shared_ptr<status_code>> recv_begin_ack_status_map;


void send_begin_callback_c(void * request, ucs_status_t status) {
	try
	{
		std::cout<<"send_beging_callback"<<std::endl;
		auto blazing_request = reinterpret_cast<ucx_request *>(request);

		std::cout<<"request "<<request<<std::endl;
		if(message_uid_to_buffer_transport.find(blazing_request->uid) == message_uid_to_buffer_transport.end()){
			std::cout<<"Call back requesting buffer that doesn't exist!!!"<<std::endl;
		}
		auto transport = message_uid_to_buffer_transport[blazing_request->uid];
		transport->recv_begin_transmission_ack();
		ucp_request_release(request);


		// blazing_request->completed = 1;
	}
	catch(const std::exception& e)
	{
		std::cerr << "Error in send_begin_callback_c: " << e.what() << '\n';
	}
}

void recv_begin_ack_callback_c(void * request, ucs_status_t status, ucp_tag_recv_info_t * info) {
	try
	{
		std::cout<<"recieve_beging_ack_callback"<<std::endl;
		std::shared_ptr<status_code> status_begin_ack = recv_begin_ack_status_map[info->sender_tag];

		auto blazing_request = reinterpret_cast<ucx_request *>(request);
		auto transport = message_uid_to_buffer_transport[blazing_request->uid];
		transport->increment_begin_transmission();

		ucp_request_release(request);

		// *status_begin_ack = status_code::OK;
	}
	catch(const std::exception& e)
	{
		std::cerr << "Error in recv_begin_ack_callback_c: " << e.what() << '\n';
	}
}

void send_callback_c(void * request, ucs_status_t status) {
	try{
		std::cout<<"send_callback"<<std::endl;
		auto blazing_request = reinterpret_cast<ucx_request *>(request);
		auto transport = message_uid_to_buffer_transport[blazing_request->uid];
		transport->increment_frame_transmission();
		ucp_request_release(request);
	}
	catch(const std::exception& e)
	{
		std::cerr << "Error in send_callback_c: " << e.what() << '\n';
	}
}


ucx_buffer_transport::ucx_buffer_transport(ucp_worker_h origin_node,
    std::vector<node> destinations,
	ral::cache::MetadataDictionary metadata,
	std::vector<size_t> buffer_sizes,
	std::vector<blazingdb::transport::ColumnTransport> column_transports,
	int ral_id)
	: ral_id{ral_id}, buffer_transport(metadata, buffer_sizes, column_transports), transmitted_begin_frames(0), transmitted_frames(0),
	  origin_node(origin_node), destinations{destinations} {
	tag = generate_message_tag();
}

ucx_buffer_transport::~ucx_buffer_transport() {
	message_uid_to_buffer_transport.erase(this->message_id);
}

/**
 * A struct for managing the 64 bit tag that ucx uses
 * This allow us to make a value that is stored in 8 bytes
 */
struct blazing_ucp_tag {
	int message_id;			   /**< The message id which is generated by a global atomic*/
	uint16_t worker_origin_id; /**< The id to make sure each tag is unique */
	uint16_t frame_id;		   /**< The id of the frame being sent. 0 for being_transmission*/
};

std::atomic<int> atomic_message_id(0);

ucp_tag_t ucx_buffer_transport::generate_message_tag() {
	std::cout<<"generating tag"<<std::endl;
	auto current_message_id = atomic_message_id.fetch_add(1);
	blazing_ucp_tag blazing_tag = {current_message_id, ral_id, 0u};
	message_uid_to_buffer_transport[blazing_tag.message_id] = this;
	this->message_id = blazing_tag.message_id;
	std::cout<<"almost done"<<std::endl;
	return *reinterpret_cast<ucp_tag_t *>(&blazing_tag);
}

void ucx_buffer_transport::send_begin_transmission() {

	std::cout<<"sending begin transmission"<<std::endl;
	std::vector<char> buffer_to_send = detail::serialize_metadata_and_transports(metadata, column_transports);

	std::vector<char *> requests(destinations.size());
	int i = 0;
	for(auto const & node : destinations) {
		requests[i] = new char[req_size + sizeof(ucx_request) + 1];
		auto status = ucp_tag_send_nbr(
			node.get_ucp_endpoint(), buffer_to_send.data(), buffer_to_send.size(), ucp_dt_make_contig(1), tag, requests[i] + req_size - sizeof(ucx_request));

		if (status == UCS_INPROGRESS) {
			do {
				ucp_worker_progress(origin_node);
    		status = ucp_request_check_status(requests[i] + req_size - sizeof(ucx_request));
			} while (status == UCS_INPROGRESS);
		}
		if(status != UCS_OK){
			throw std::runtime_error("Was not able to send begin transmission to " + node.id());
		}

		std::cout<< "send begin transmition done" << std::endl;

		status_code recv_begin_status = status_code::INVALID;
		ucp_tag_recv_info_t info_tag;
		blazing_ucp_tag acknowledge_tag = *reinterpret_cast<blazing_ucp_tag *>(&tag);
		acknowledge_tag.frame_id = 0xFFFF;



		std::cout<< "recv_begin_transmission_ack recv_nb" << std::endl;
		requests[i] = new char[req_size + sizeof(ucx_request) + 1];
		status = ucp_tag_recv_nbr(origin_node,
									&recv_begin_status,
									sizeof(status_code),
									ucp_dt_make_contig(1),
									*reinterpret_cast<ucp_tag_t *>(&acknowledge_tag),
									acknownledge_tag_mask,
									requests[i] + req_size - sizeof(ucx_request));

		if (status == UCS_INPROGRESS) {
			do {
				ucp_worker_progress(origin_node);
				ucp_tag_recv_info_t info_tag;
    		status = ucp_tag_recv_request_test(requests[i] + req_size - sizeof(ucx_request), &info_tag);
			} while (status == UCS_INPROGRESS);
		}
		if(status != UCS_OK){
			throw std::runtime_error("Was not able to receive acknowledgment of begin transmission from " + node.id());
		}
		i++;
		std::cout<<"got ack!!!!"<<std::endl;
		increment_begin_transmission();
	}
}

void ucx_buffer_transport::increment_frame_transmission() {
	transmitted_frames++;
	std::cout<<"Increment begin transmission"<<std::endl;	
	completion_condition_variable.notify_all();
}

void ucx_buffer_transport::increment_begin_transmission() {
	transmitted_begin_frames++;
	completion_condition_variable.notify_all();
	std::cout<<"Increment begin transmission"<<std::endl;
}

void ucx_buffer_transport::wait_for_begin_transmission() {
	std::unique_lock<std::mutex> lock(mutex);
	completion_condition_variable.wait(lock, [this] {
		if(transmitted_begin_frames >= destinations.size()) {
			return true;
		} else {
			return false;
		}
	});
	std::cout<< "FINISHED WAITING wait_for_begin_transmission"<<std::endl;
}

void ucx_buffer_transport::recv_begin_transmission_ack() {
	std::cout<<"going to receive ack!!"<<std::endl;
	auto recv_begin_status = std::make_shared<status_code>(status_code::INVALID);
	std::shared_ptr<ucp_tag_recv_info_t> info_tag = std::make_shared<ucp_tag_recv_info_t>();
	blazing_ucp_tag acknowledge_tag = *reinterpret_cast<blazing_ucp_tag *>(&tag);
	acknowledge_tag.frame_id = 0xFFFF;

	ucp_worker_h ucp_worker = origin_node;
	ucp_tag_message_h message_tag;

	for(;;) {
		// std::cout<<"inside loop for probe!!"<<std::endl;
		message_tag = ucp_tag_probe_nb(ucp_worker,
			*reinterpret_cast<ucp_tag_t *>(&acknowledge_tag),
			acknownledge_tag_mask, 1, info_tag.get());

		if (message_tag != NULL) {
				/* Message arrived */
				break;
		}

		// else if (ucp_worker_progress(ucp_worker)) {
		// 		/* Some events were polled; try again without going to sleep */
		// 		continue;
		// }
	}

	std::cout<< "recv_begin_transmission_ack recv_nb" << std::endl;
	recv_begin_ack_status_map[tag] = recv_begin_status;
	auto request = ucp_tag_msg_recv_nb(ucp_worker,
																		recv_begin_status.get(),
																		info_tag->length,
																		ucp_dt_make_contig(1),
																		message_tag,
																		recv_begin_ack_callback_c);

	if(UCS_PTR_IS_ERR(request)) {
		// TODO: decide how to do cleanup i think we just throw an initialization exception
	} else if(UCS_PTR_STATUS(request) == UCS_OK){
		increment_begin_transmission();
	}	else {
		// assert(UCS_PTR_IS_PTR(request));
    // while (*recv_begin_status != status_code::OK) {
    //     ucp_worker_progress(ucp_worker);
    // }
		// auto transport = message_uid_to_buffer_transport[blazing_request->uid];
		// transport->increment_begin_transmission();

		// ucp_request_release(request);

		auto blazing_request = reinterpret_cast<ucx_request *>(request);
		blazing_request->uid = reinterpret_cast<blazing_ucp_tag *>(&tag)->message_id;

	}
}

void ucx_buffer_transport::wait_until_complete() {
	std::unique_lock<std::mutex> lock(mutex);
	completion_condition_variable.wait(lock, [this] {
		if(transmitted_frames >= (buffer_sizes.size() * destinations.size())) {
			return true;
		} else {
			return false;
		}
	});
}

void ucx_buffer_transport::send_impl(const char * buffer, size_t buffer_size) {
	std::vector<ucs_status_ptr_t> requests;
	blazing_ucp_tag blazing_tag = *reinterpret_cast<blazing_ucp_tag *>(&tag);
	blazing_tag.frame_id = buffer_sent + 1;	 // 0th position is for the begin_message
	for(auto const & node : destinations) {
		requests.push_back(ucp_tag_send_nb(node.get_ucp_endpoint(),
			buffer,
			buffer_size,
			ucp_dt_make_contig(1),
			*reinterpret_cast<ucp_tag_t *>(&blazing_tag),
			send_callback_c));
	}

	for(auto & request : requests) {
		if(UCS_PTR_IS_ERR(request)) {
			// TODO: decide how to do cleanup i think we just throw an initialization exception
		} else if(UCS_PTR_STATUS(request) == UCS_OK) {
			increment_frame_transmission();
		} else {
			// Message was not completed we set the uid for the callback
			auto blazing_request = reinterpret_cast<ucx_request *>(&request);
			blazing_request->uid = reinterpret_cast<blazing_ucp_tag *>(&tag)->message_id;
		}
	}
	// TODO: call ucp_worker_progress here
}

ucx_message_listener::ucx_message_listener(ucp_worker_h worker, int num_threads) :
    ucp_worker(worker), pool{num_threads} {

}

std::map<void *,std::shared_ptr<status_code> > status_scope_holder;

void send_acknowledge_callback_c(void * request, ucs_status_t status){
	try{
		std::cout<<"send ack callback"<<std::endl;
		status_scope_holder.erase(request);
		ucp_request_release(request);
	}
	catch(const std::exception& e)
	{
		std::cerr << "Error in send_acknowledge_callback_c: " << e.what() << '\n';
	}
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


ctpl::thread_pool<BlazingThread> & ucx_message_listener::get_pool(){
	return pool;
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
			receiver->set_buffer_size(position,info_tag->length);

			auto request = ucp_tag_msg_recv_nb(ucp_worker,
																				receiver->get_buffer(position),
																				info_tag->length,
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


static void flush_callback(void *request, ucs_status_t status)
{
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
		std::cout<<"BEFORE get_metadata_and_transports_from_bytes"<<std::endl;
		auto metadata_and_transports = detail::get_metadata_and_transports_from_bytes(buffer);
		std::cout<<"buffer "<<std::endl<<std::string(buffer.begin(),buffer.end())<<std::endl;
		auto metadata = metadata_and_transports.first;
		metadata.print();
		std::cout<<"got meta and transports"<<std::endl;
		int32_t ctx_token = std::stoi(metadata.get_values()[ral::cache::QUERY_ID_METADATA_LABEL]);
		std::cout<<"getting graph"<<std::endl;
		auto graph = graphs_info::getInstance().get_graph(ctx_token);
		std::cout<<"got graph "<<graph<<std::endl;
		size_t kernel_id = std::stoull(metadata.get_values()[ral::cache::KERNEL_ID_METADATA_LABEL]);
		std::string cache_id = metadata.get_values()[ral::cache::CACHE_ID_METADATA_LABEL];
				std::cout<<"getting cache"<<std::endl;
		auto out_cache = metadata.get_values()[ral::cache::ADD_TO_SPECIFIC_CACHE_METADATA_LABEL] == "true" ?
				graph->get_kernel_output_cache(kernel_id, cache_id) : graph->get_input_message_cache();

		std::cout<<"got cache"<<std::endl;
		auto receiver = std::make_shared<message_receiver>(
			metadata_and_transports.second,
			metadata,
			out_cache);
				std::cout<<"madd receiver"<<std::endl;
		message_listener->add_receiver(info->sender_tag, receiver);
				std::cout<<"registered receiver"<<std::endl;
		//TODO: if its a specific cache get that cache adn put it here else put the general iput cache from the graph
		auto node = ucp_nodes_info::getInstance().get_node(metadata.get_values()[ral::cache::SENDER_WORKER_ID_METADATA_LABEL]);
		std::cout<<"got node"<<std::endl;
		auto acknowledge_tag = *reinterpret_cast<blazing_ucp_tag *>(&info->sender_tag);
		acknowledge_tag.frame_id = 0xFFFF;
		auto acknowledge_tag_ucp = *reinterpret_cast<ucp_tag_t *>(&acknowledge_tag);

		auto status_acknowledge = std::make_shared<status_code>(status_code::OK);
		std::cout<<"about to send ack"<<std::endl;

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

void ucx_message_listener::initialize_message_listener(ucp_worker_h worker, int num_threads){
	if(instance == NULL) {
		instance = new ucx_message_listener(worker,num_threads);
	}
}
ucx_message_listener * ucx_message_listener::get_instance() {
	if(instance == NULL) {
		throw::std::exception();
	}
	return instance;
}

}  // namespace comm
