#include "BlazingInputStream.h"


BlazingInputStream::BlazingInputStream(std::shared_ptr<arrow::io::RandomAccessFile> arrow_file_handle, std::string file_path) :
    arrow_file_handle(arrow_file_handle), file_name(file_path){}

BlazingInputStream::~BlazingInputStream(){}

/**
 * Get the total length of the file in bytes.
 */
uint64_t BlazingInputStream::getLength() const {
    return arrow_file_handle->GetSize().ValueOrDie();
}

/**
 * Get the natural size for reads.
 * @return the number of bytes that should be read at once
 */
uint64_t BlazingInputStream::getNaturalReadSize() const {
    return this->getLength() > DEFAULT_NATURAL_READ_SIZE ? DEFAULT_NATURAL_READ_SIZE : this->getLength(); 
}

/**
 * Read length bytes from the file starting at offset into
 * the buffer starting at buf.
 * @param buf the starting position of a buffer.
 * @param length the number of bytes to read.
 * @param offset the position in the stream to read from.
 */
void BlazingInputStream::read(void* buf,
                uint64_t length,
                uint64_t offset) {
    
    uint64_t length_in = length;
    int64_t total_bytes_read = 0;

    while (total_bytes_read < length_in){
        auto bytes_read_result = arrow_file_handle->ReadAt(offset, length, buf);
        int64_t bytes_read = bytes_read_result.ValueOrDie();
        if (bytes_read == 0){
            std::cout<<"ERROR: reading from file "<<this->file_name<<" total_bytes_read: "<<total_bytes_read<<" from "<<length_in<<" requested"<<std::endl;
        }
        total_bytes_read += bytes_read;
        offset += bytes_read;
        length -= bytes_read;
    }
}

/**
 * Get the name of the stream for error messages.
 */
const std::string& BlazingInputStream::getName() const {
    return file_name;
}

