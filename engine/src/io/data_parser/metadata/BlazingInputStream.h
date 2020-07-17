#pragma once

#include <string>
#include <memory>

#include <orc/OrcFile.hh>
#include <arrow/io/file.h>

// TODO need to set this more appropriatelly. This is right now the default HDFS read size
const uint64_t DEFAULT_NATURAL_READ_SIZE = 64000000; 

class BlazingInputStream : orc::InputStream {
  public:

    BlazingInputStream(std::shared_ptr<arrow::io::RandomAccessFile> arrow_file_handle, std::string file_path);

    virtual ~BlazingInputStream();

    /**
     * Get the total length of the file in bytes.
     */
    uint64_t getLength() const;

    /**
     * Get the natural size for reads.
     * @return the number of bytes that should be read at once
     */
    uint64_t getNaturalReadSize() const;

    /**
     * Read length bytes from the file starting at offset into
     * the buffer starting at buf.
     * @param buf the starting position of a buffer.
     * @param length the number of bytes to read.
     * @param offset the position in the stream to read from.
     */
    void read(void* buf,
                    uint64_t length,
                    uint64_t offset);

    /**
     * Get the name of the stream for error messages.
     */
    const std::string& getName() const;

    private: 
        std::shared_ptr<arrow::io::RandomAccessFile> arrow_file_handle;
        std::string file_name;
};