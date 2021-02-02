#include "bufferTransport.hpp"
#include "CodeTimer.h"

using namespace std::chrono_literals;
using namespace fmt::literals;

namespace comm {

namespace detail {

std::vector<char> serialize_metadata_and_transports_and_buffer_sizes(const ral::cache::MetadataDictionary & metadata,
                                                    const std::vector<blazingdb::transport::ColumnTransport> & column_transports,
													const std::vector<ral::memory::blazing_chunked_column_info> & chunked_column_infos,
                                                    const std::vector<size_t> buffer_sizes) {
	// builds the cpu host buffer that we are going to send
	// first lets serialize and send metadata
	std::string metadata_buffer;
	for(auto it : metadata.get_values()) {
		metadata_buffer += it.first + "%==%" + it.second + "\n";
	}

	std::vector<char> buffer, tmp_buffer;
	tmp_buffer = detail::to_byte_vector(metadata_buffer.size());
	buffer.insert(buffer.end(), tmp_buffer.begin(), tmp_buffer.end());

	buffer.insert(buffer.end(), metadata_buffer.begin(), metadata_buffer.end());

	tmp_buffer = detail::to_byte_vector(column_transports.size()); // tells us how many transports will be sent
	buffer.insert(buffer.end(), tmp_buffer.begin(), tmp_buffer.end());

	tmp_buffer = detail::vector_to_byte_vector(column_transports);
	buffer.insert(buffer.end(), tmp_buffer.begin(), tmp_buffer.end());

	// lets serialize now the chunked_column_infos
	tmp_buffer = detail::to_byte_vector(chunked_column_infos.size());  // tells us how many chunked_column_infos there are
	buffer.insert(buffer.end(), tmp_buffer.begin(), tmp_buffer.end());
	for (const auto &chunked_column_info : chunked_column_infos){
		// first we serialize chunk_index
		tmp_buffer = detail::to_byte_vector(chunked_column_info.chunk_index.size()); 
		buffer.insert(buffer.end(), tmp_buffer.begin(), tmp_buffer.end());

		tmp_buffer = detail::vector_to_byte_vector(chunked_column_info.chunk_index);
		buffer.insert(buffer.end(), tmp_buffer.begin(), tmp_buffer.end());

		// then we serialize offset
		tmp_buffer = detail::to_byte_vector(chunked_column_info.offset.size()); 
		buffer.insert(buffer.end(), tmp_buffer.begin(), tmp_buffer.end());

		tmp_buffer = detail::vector_to_byte_vector(chunked_column_info.offset);
		buffer.insert(buffer.end(), tmp_buffer.begin(), tmp_buffer.end());

		// then we serialize size
		tmp_buffer = detail::to_byte_vector(chunked_column_info.size.size()); 
		buffer.insert(buffer.end(), tmp_buffer.begin(), tmp_buffer.end());

		tmp_buffer = detail::vector_to_byte_vector(chunked_column_info.size);
		buffer.insert(buffer.end(), tmp_buffer.begin(), tmp_buffer.end());

		// finally we serialize use_size
		tmp_buffer = detail::to_byte_vector(chunked_column_info.use_size); 
		buffer.insert(buffer.end(), tmp_buffer.begin(), tmp_buffer.end());
	}

	tmp_buffer = detail::to_byte_vector(buffer_sizes.size()); // tells us how many buffers will be sent
	buffer.insert(buffer.end(), tmp_buffer.begin(), tmp_buffer.end());

	tmp_buffer = detail::vector_to_byte_vector(buffer_sizes);
	buffer.insert(buffer.end(), tmp_buffer.begin(), tmp_buffer.end());

	return buffer;
}


std::tuple<ral::cache::MetadataDictionary, std::vector<blazingdb::transport::ColumnTransport>, 
			std::vector<ral::memory::blazing_chunked_column_info>, std::vector<size_t> >
									get_metadata_and_transports_and_buffer_sizes_from_bytes(std::vector<char> data){

	std::cout<<"get_metadata_and_transports_and_buffer_sizes_from_bytes start"<<std::endl;
    size_t ptr_offset = 0;

	// first lets deserialize the metadata
	size_t metadata_buffer_size = from_byte_vector<size_t>(data.data());
	ptr_offset += sizeof(size_t);

	std::string metadata_buffer(
		data.data() + ptr_offset,
		data.data() + ptr_offset + metadata_buffer_size);

	ptr_offset += metadata_buffer_size;
	ral::cache::MetadataDictionary dictionary;
	for(auto metadata_item : StringUtil::split(metadata_buffer,"\n")){
		if (metadata_item.empty()) {
			continue;
		}

		std::vector<std::string> key_value = StringUtil::split(metadata_item,"%==%");
		if(key_value.size() == 1){
			dictionary.add_value(key_value[0],"");
		}else{
			dictionary.add_value(key_value[0],key_value[1]);
		}
	}

	// next lets deserialize column_transports
	size_t column_transports_size = from_byte_vector<size_t>(data.data() + ptr_offset);
	ptr_offset += sizeof(size_t);
	auto column_transports = vector_from_byte_vector<blazingdb::transport::ColumnTransport>(
		data.data() + ptr_offset, column_transports_size);
    ptr_offset += column_transports_size * sizeof(blazingdb::transport::ColumnTransport);


	// next we deserialize chunked_column_infos
	size_t num_chunked_column_info = from_byte_vector<size_t>(data.data() + ptr_offset);
	ptr_offset += sizeof(size_t);
	std::vector<ral::memory::blazing_chunked_column_info> chunked_column_infos(num_chunked_column_info);
	for (auto i = 0; i < num_chunked_column_info; i++){
		// first we deserialize chunk_index
		size_t num_chunk_index = from_byte_vector<size_t>(data.data() + ptr_offset);
		ptr_offset += sizeof(size_t);
		chunked_column_infos[i].chunk_index = vector_from_byte_vector<size_t>(data.data() + ptr_offset, num_chunk_index);
		ptr_offset += num_chunk_index * sizeof(size_t);

		// then we deserialize offset
		size_t num_offset = from_byte_vector<size_t>(data.data() + ptr_offset);
		ptr_offset += sizeof(size_t);
		chunked_column_infos[i].offset = vector_from_byte_vector<size_t>(data.data() + ptr_offset, num_offset);
		ptr_offset += num_offset * sizeof(size_t);

		// then we deserialize size
		size_t num_size = from_byte_vector<size_t>(data.data() + ptr_offset);
		ptr_offset += sizeof(size_t);
		chunked_column_infos[i].size = vector_from_byte_vector<size_t>(data.data() + ptr_offset, num_size);
		ptr_offset += num_size * sizeof(size_t);

		// finally we deserialize use_size
		chunked_column_infos[i].use_size = from_byte_vector<size_t>(data.data() + ptr_offset);
		ptr_offset += sizeof(size_t);
	}


	// finally we deserialize buffer_sizes
    size_t buffer_size = from_byte_vector<size_t>(
		data.data() + ptr_offset);
	ptr_offset += sizeof(size_t);
	auto buffer_sizes = vector_from_byte_vector<size_t>(
		data.data() + ptr_offset, buffer_size);
	

	std::cout<<"get_metadata_and_transports_and_buffer_sizes_from_bytes end"<<std::endl;
	
	return std::make_tuple(dictionary, column_transports, chunked_column_infos, buffer_sizes);
}

} // namespace detail

buffer_transport::buffer_transport(ral::cache::MetadataDictionary metadata,
  std::vector<size_t> buffer_sizes,
  std::vector<blazingdb::transport::ColumnTransport> column_transports, std::vector<ral::memory::blazing_chunked_column_info> chunked_column_infos, std::vector<node> destinations, bool require_acknowledge)
  : column_transports{column_transports}, chunked_column_infos{chunked_column_infos}, metadata{metadata}, buffer_sizes{buffer_sizes}, transmitted_begin_frames(0), transmitted_frames(0),
	 destinations{destinations} , require_acknowledge{require_acknowledge}  {
  // iterate for workers this is destined for

	if(require_acknowledge){
		for (const auto & destination : destinations){
			transmitted_acknowledgements[destination.id()] = false;
		}
	}

}

buffer_transport::~buffer_transport(){

}

void buffer_transport::send(const char * buffer, size_t buffer_size){
  send_impl(buffer, buffer_size);
  buffer_sent++;
}



void buffer_transport::increment_frame_transmission() {
	transmitted_frames++;
	completion_condition_variable.notify_all();
}

void buffer_transport::increment_begin_transmission() {
	transmitted_begin_frames++;
	completion_condition_variable.notify_all();
}

void buffer_transport::wait_for_begin_transmission() {

	CodeTimer blazing_timer;
	std::unique_lock<std::mutex> lock(mutex);
	while(!completion_condition_variable.wait_for(lock, 1000ms, [&blazing_timer, this] {
		bool done_waiting = transmitted_begin_frames >= destinations.size();
		if (!done_waiting && blazing_timer.elapsed_time() > 990) {
            std::shared_ptr<spdlog::logger> logger = spdlog::get("batch_logger");
			if(logger) {
				logger->warn("|||{info}|{duration}||||",
									"info"_a="buffer_transport::wait_for_begin_transmission() timed out. transmitted_begin_frames: " + std::to_string(transmitted_begin_frames) + " destinations.size(): " + std::to_string(destinations.size()),
									"duration"_a=blazing_timer.elapsed_time());
			}
		}
		return done_waiting;
	})){}
}


void buffer_transport::wait_until_complete() {
	for(const auto destination : destinations){

	}
	CodeTimer blazing_timer;
	std::unique_lock<std::mutex> lock(mutex);
	while(!completion_condition_variable.wait_for(lock, 1000ms, [&blazing_timer, this] {
		bool done_waiting = transmitted_frames >= (buffer_sizes.size() * destinations.size());
		if(require_acknowledge){
			done_waiting = done_waiting && std::all_of(transmitted_acknowledgements.begin(), transmitted_acknowledgements.end(), [](const auto& elem) { return elem.second; });
		}
		if (!done_waiting && blazing_timer.elapsed_time() > 990) {
			std::string missing_parts;
			std::for_each(transmitted_acknowledgements.begin(),
                transmitted_acknowledgements.end(),
                [&missing_parts](const std::pair<std::string,bool> &elem) {
                    if(!elem.second){
						missing_parts += elem.first + ",";
					}
                });
			std::shared_ptr<spdlog::logger> logger = spdlog::get("batch_logger");
			if(logger) {
				logger->warn("|||{info}|{duration}|{missing_parts}|||",
									"info"_a="buffer_transport::wait_until_complete() timed out. transmitted_frames: " + std::to_string(transmitted_frames) + " buffer_sizes.size(): " + std::to_string(buffer_sizes.size()) + " destinations.size(): " + std::to_string(destinations.size()),
									"missing_parts"_a=missing_parts,
									"duration"_a=blazing_timer.elapsed_time());
			}
		}
		return done_waiting;
	})){}
}

} // namespace comm
