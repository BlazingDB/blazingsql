cdef extern from "../src/io/data_parser/ParquetParser.cpp":
    pass

# Declare the class with cdef
cdef extern from "../src/io/data_parser/ParquetParser.h" namespace "ral::io":
    cdef cppclass parquet_parser:
        parquet_parser() except +
        void parse(std::shared_ptr<arrow::io::RandomAccessFile> file, const std::string & user_readable_file_handle, std::vector<gdf_column_cpp> & columns_out, const Schema & schema,std::vector<size_t> column_indices_requested) except +
        void parse_schema(std::vector<std::shared_ptr<arrow::io::RandomAccessFile> > files, Schema & schema) except +
