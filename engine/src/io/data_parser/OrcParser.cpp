#include "OrcParser.h"

#include <arrow/io/file.h>

#include <blazingdb/io/Library/Logging/Logger.h>
#include "blazingdb/concurrency/BlazingThread.h"

#include <orc/OrcFile.hh>
#include <orc/Reader.hh>
#include <orc/Statistics.hh>

#include <numeric>

namespace ral {
namespace io {

orc_parser::orc_parser(cudf::io::read_orc_args arg_) : orc_args{arg_} {}

orc_parser::~orc_parser() {
	// TODO Auto-generated destructor stub
}

cudf_io::table_with_metadata get_new_orc(cudf_io::read_orc_args orc_arg,
	std::shared_ptr<arrow::io::RandomAccessFile> arrow_file_handle,
	bool first_row_only = false){

	orc_arg.source = cudf_io::source_info(arrow_file_handle);

	if (first_row_only)
		orc_arg.num_rows = 1;

	cudf_io::table_with_metadata table_out = cudf_io::read_orc(orc_arg);

	arrow_file_handle->Close();

	return std::move(table_out);
}

std::unique_ptr<ral::frame::BlazingTable> orc_parser::parse_batch(
	std::shared_ptr<arrow::io::RandomAccessFile> file,
	const Schema & schema,
	std::vector<size_t> column_indices,
	std::vector<cudf::size_type> row_groups)
{
	if(file == nullptr) {
		return schema.makeEmptyBlazingTable(column_indices);
	}
	if(column_indices.size() > 0) {
		// Fill data to orc_args
		cudf_io::read_orc_args orc_args{cudf_io::source_info{file}};

		orc_args.columns.resize(column_indices.size());

		for(size_t column_i = 0; column_i < column_indices.size(); column_i++) {
			orc_args.columns[column_i] = schema.get_name(column_indices[column_i]);
		}

		orc_args.stripe_list = row_groups;

		auto result = cudf_io::read_orc(orc_args);
		return std::make_unique<ral::frame::BlazingTable>(std::move(result.tbl), result.metadata.column_names);
	}
	return nullptr;
}

void orc_parser::parse_schema(
	std::shared_ptr<arrow::io::RandomAccessFile> file, ral::io::Schema & schema) {

	cudf_io::table_with_metadata table_out = get_new_orc(orc_args, file, true);

	for(cudf::size_type i = 0; i < table_out.tbl->num_columns() ; i++) {
		std::string name = table_out.metadata.column_names[i];
		cudf::type_id type = table_out.tbl->get_column(i).type().id();
		size_t file_index = i;
		bool is_in_file = true;
		schema.add_column(name, type, file_index, is_in_file);
	}
}

std::unique_ptr<ral::frame::BlazingTable> orc_parser::get_metadata(std::vector<std::shared_ptr<arrow::io::RandomAccessFile>> files, 
		std::vector<std::string> file_paths, int offset){

	std::vector<size_t> num_row_groups(files.size());
	BlazingThread threads[files.size()];
	// std::vector<std::unique_ptr<orc::InputStream>> orc_input_streams(files.size()); 

	std::unique_ptr<orc::InputStream> orc_input_stream = orc::readLocalFile(file_paths[0]);

	orc::ReaderOptions reader_options = orc::ReaderOptions();
	std::unique_ptr<orc::Reader> orc_reader = orc::createReader(std::move(orc_input_stream), reader_options);

	uint64_t num_rows = orc_reader->getNumberOfRows();
	std::cout<<"getNumberOfRows: "<<num_rows<<std::endl;
	std::list<std::string> metadata_keys = orc_reader->getMetadataKeys();
	std::cout<<"num metadata keys: "<<metadata_keys.size()<<std::endl;
	for (int i =0; i < metadata_keys.size(); i++){
		std::string key = metadata_keys.front();
		metadata_keys.pop_front();
		std::string value = orc_reader->getMetadataValue(key);
		std::cout<<"metadata "<<i<<" key: "<<key<<" value: "<<value<<std::endl;
	}
	
	uint64_t row_index_stride = orc_reader->getRowIndexStride();
	std::cout<<"row_index_stride: "<<row_index_stride<<std::endl;
	uint64_t num_striped = orc_reader->getNumberOfStripes();
	std::cout<<"num_striped: "<<num_striped<<std::endl;
	uint64_t num_stripe_statistics = orc_reader->getNumberOfStripeStatistics();
	std::cout<<"num_stripe_statistics: "<<num_stripe_statistics<<std::endl;
	bool correct_statistics = orc_reader->hasCorrectStatistics();
	std::cout<<"correct_statistics: "<<correct_statistics<<std::endl;



	// std::vector<std::unique_ptr<parquet::ParquetFileReader>> parquet_readers(files.size());
	// for(int file_index = 0; file_index < files.size(); file_index++) {
	// 	threads[file_index] = BlazingThread([&, file_index]() {
	// 	  parquet_readers[file_index] =
	// 		  std::move(parquet::ParquetFileReader::Open(files[file_index]));
	// 	  std::shared_ptr<parquet::FileMetaData> file_metadata = parquet_readers[file_index]->metadata();
	// 	  const parquet::SchemaDescriptor * schema = file_metadata->schema();
	// 	  num_row_groups[file_index] = file_metadata->num_row_groups();
	// 	});
	// }

	// for(int file_index = 0; file_index < files.size(); file_index++) {
	// 	threads[file_index].join();
	// }

	// size_t total_num_row_groups =
	// 	std::accumulate(num_row_groups.begin(), num_row_groups.end(), size_t(0));

	// auto minmax_metadata_table = get_minmax_metadata(parquet_readers, total_num_row_groups, offset);
	// for (auto &reader : parquet_readers) {
	// 	reader->Close();
	// }
	// return std::move(minmax_metadata_table);
}

} /* namespace io */
} /* namespace ral */
