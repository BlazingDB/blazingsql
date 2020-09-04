#include "bufferTransport.hpp"

namespace comm {

namespace detail {

std::vector<char> serialize_metadata_and_transports_and_buffer_sizes(const ral::cache::MetadataDictionary & metadata,
                                                    const std::vector<blazingdb::transport::ColumnTransport> & column_transports,
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

  tmp_buffer = detail::to_byte_vector(buffer_sizes.size()); // tells us how many buffers will be sent
  buffer.insert(buffer.end(), tmp_buffer.begin(), tmp_buffer.end());

  tmp_buffer = detail::vector_to_byte_vector(buffer_sizes);
  buffer.insert(buffer.end(), tmp_buffer.begin(), tmp_buffer.end());



  return buffer;
}

std::tuple<ral::cache::MetadataDictionary, std::vector<blazingdb::transport::ColumnTransport>, std::vector<size_t> >
get_metadata_and_transports_and_buffer_sizes_from_bytes(std::vector<char> data){
    size_t ptr_offset = 0;
	size_t metadata_buffer_size = from_byte_vector<size_t>(data.data());
	ptr_offset += sizeof(size_t);
std::cout<<"before metadata_buffer: " <<data.size() << " ptr_offset:  "<< ptr_offset<< " metadata_buffer_size "<< metadata_buffer_size <<std::endl;
	std::string metadata_buffer(
		data.data() + ptr_offset,
		data.data() + ptr_offset + metadata_buffer_size);

  std::cout<<"metadata buffer size is : "<<metadata_buffer.size()<<std::endl;
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

	size_t column_transports_size = from_byte_vector<size_t>(
		data.data() + ptr_offset);
	ptr_offset += sizeof(size_t);
	auto column_transports = vector_from_byte_vector<blazingdb::transport::ColumnTransport>(
		data.data() + ptr_offset, column_transports_size);
  ptr_offset += column_transports_size * sizeof(blazingdb::transport::ColumnTransport);


  size_t buffer_size = from_byte_vector<size_t>(
		data.data() + ptr_offset);
	ptr_offset += sizeof(size_t);
	auto buffer_sizes = vector_from_byte_vector<size_t>(
		data.data() + ptr_offset, buffer_size);

	return std::make_tuple(dictionary,column_transports,buffer_sizes);
}

} // namespace detail

buffer_transport::buffer_transport(ral::cache::MetadataDictionary metadata,
  std::vector<size_t> buffer_sizes,
  std::vector<blazingdb::transport::ColumnTransport> column_transports, std::vector<node> destinations)
  : column_transports{column_transports}, buffer_sizes{buffer_sizes}, metadata{metadata}, transmitted_begin_frames(0), transmitted_frames(0),
	 destinations{destinations}   {
  // iterate for workers this is destined for

}

buffer_transport::~buffer_transport(){
	
}

void buffer_transport::send(const char * buffer, size_t buffer_size){
  send_impl(buffer, buffer_size);
  buffer_sent++;
}



void buffer_transport::increment_frame_transmission() {
	transmitted_frames++;
	std::cout<<"Increment begin transmission"<<std::endl;
	completion_condition_variable.notify_all();
}

void buffer_transport::increment_begin_transmission() {
	transmitted_begin_frames++;
	completion_condition_variable.notify_all();
	std::cout<<"Increment begin transmission"<<std::endl;
}

void buffer_transport::wait_for_begin_transmission() {
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


void buffer_transport::wait_until_complete() {
	std::unique_lock<std::mutex> lock(mutex);
	completion_condition_variable.wait(lock, [this] {
		if(transmitted_frames >= (buffer_sizes.size() * destinations.size())) {
			return true;
		} else {
			return false;
		}
	});
  std::cout<< "FINISHED WAITING wait_until_complete"<<std::endl;
}

} // namespace comm
