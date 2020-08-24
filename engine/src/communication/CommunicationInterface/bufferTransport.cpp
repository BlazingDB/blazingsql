#include "bufferTransport.hpp"

namespace comm {

namespace detail {

std::pair<ral::cache::MetadataDictionary, std::vector<blazingdb::transport::ColumnTransport>>
get_metadata_and_transports_from_bytes(std::vector<char> data){
    size_t ptr_offset = 0;
	size_t metadata_buffer_size = from_byte_vector<size_t>(data.data());
	ptr_offset += sizeof(size_t);

	std::string metadata_buffer(
		data.data() + ptr_offset,
		data.data() + ptr_offset + metadata_buffer_size);

  std::cout<<"metadata buffer is"<<metadata_buffer<<std::endl;
	ptr_offset += metadata_buffer_size;
	ral::cache::MetadataDictionary dictionary;
	for(auto metadata_item : StringUtil::split(metadata_buffer,"\n")){
    
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
	return std::make_pair(dictionary,column_transports);
}

} // namespace detail

buffer_transport::buffer_transport(ral::cache::MetadataDictionary metadata,
  std::vector<size_t> buffer_sizes,
  std::vector<blazingdb::transport::ColumnTransport> column_transports)
  : column_transports{column_transports}, buffer_sizes{buffer_sizes}, metadata{metadata} {
  // iterate for workers this is destined for

}

buffer_transport::~buffer_transport(){}

void buffer_transport::send(const char * buffer, size_t buffer_size){
  send_impl(buffer, buffer_size);
  buffer_sent++;
}

std::vector<char> buffer_transport::make_begin_transmission() {
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

  tmp_buffer = detail::to_byte_vector(column_transports.size()); // tells us how many befores will be sent
  buffer.insert(buffer.end(), tmp_buffer.begin(), tmp_buffer.end());

  tmp_buffer = detail::vector_to_byte_vector(column_transports);
  buffer.insert(buffer.end(), tmp_buffer.begin(), tmp_buffer.end());

  return buffer;
}

} // namespace comm
