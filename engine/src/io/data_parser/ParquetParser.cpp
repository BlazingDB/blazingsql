
#include "metadata/parquet_metadata.h"

#include "ParquetParser.h"
#include "utilities/CommonOperations.h"

#include <numeric>

#include <arrow/io/file.h>
#include "ExceptionHandling/BlazingThread.h"

#include <parquet/column_writer.h>
#include <parquet/file_writer.h>

#include <cudf/io/parquet.hpp>

namespace ral {
namespace io {

namespace cudf_io = cudf::io;

parquet_parser::parquet_parser() {
	// TODO Auto-generated constructor stub
}

parquet_parser::~parquet_parser() {
	// TODO Auto-generated destructor stub
}

std::unique_ptr<ral::frame::BlazingTable> parquet_parser::parse_batch(
	std::shared_ptr<arrow::io::RandomAccessFile> file,
	const Schema & schema,
	std::vector<int> column_indices,
	std::vector<cudf::size_type> row_groups)
{
	if(file == nullptr) {
		return schema.makeEmptyBlazingTable(column_indices);
	}
	if(column_indices.size() > 0) {
		// Fill data to pq_args
		auto arrow_source = cudf_io::arrow_io_source{file};
		cudf_io::parquet_reader_options pq_args = cudf_io::parquet_reader_options::builder(cudf_io::source_info{&arrow_source});

		pq_args.enable_convert_strings_to_categories(false);
		pq_args.enable_use_pandas_metadata(false);
		
		std::vector<std::string> col_names(column_indices.size());

		for(size_t column_i = 0; column_i < column_indices.size(); column_i++) {
			col_names[column_i] = schema.get_name(column_indices[column_i]);
		}

		pq_args.set_columns(col_names);

		pq_args.set_row_groups(std::vector<std::vector<cudf::size_type>>(1, row_groups));

		auto result = cudf::io::read_parquet(pq_args);

		auto result_table = std::move(result.tbl);
		if (result.metadata.column_names.size() > column_indices.size()) {
			auto columns = result_table->release();
			// Assuming columns are in the same order as column_indices and any extra columns (i.e. index column) are put last
			columns.resize(column_indices.size());
			result_table = std::make_unique<cudf::table>(std::move(columns));
		}

		return std::make_unique<ral::frame::BlazingTable>(std::move(result_table), result.metadata.column_names);
	}
	return nullptr;
}

void parquet_parser::parse_schema(
	std::shared_ptr<arrow::io::RandomAccessFile> file, ral::io::Schema & schema) {

	auto parquet_reader = parquet::ParquetFileReader::Open(file);
	if (parquet_reader->metadata()->num_rows() == 0) {
		parquet_reader->Close();
		return; // if the file has no rows, we dont want cudf_io to try to read it
	}

	auto arrow_source = cudf_io::arrow_io_source{file};
	cudf_io::parquet_reader_options pq_args = cudf_io::parquet_reader_options::builder(cudf_io::source_info{&arrow_source});

	pq_args.enable_convert_strings_to_categories(false);
	pq_args.enable_use_pandas_metadata(false);
	pq_args.set_num_rows(1);  // we only need the metadata, so one row is fine

	cudf_io::table_with_metadata table_out = cudf_io::read_parquet(pq_args);

	for(size_t i = 0; i < table_out.tbl->num_columns(); i++) {
		cudf::type_id type = table_out.tbl->get_column(i).type().id();
		size_t file_index = i;
		bool is_in_file = true;
		std::string name = table_out.metadata.column_names.at(i);
		schema.add_column(name, type, file_index, is_in_file);
	}
}


std::unique_ptr<ral::frame::BlazingTable> parquet_parser::get_metadata(std::vector<std::shared_ptr<arrow::io::RandomAccessFile>> files, int offset){
	std::vector<size_t> num_row_groups(files.size());
	BlazingThread threads[files.size()];
	std::vector<std::unique_ptr<parquet::ParquetFileReader>> parquet_readers(files.size());
	for(int file_index = 0; file_index < files.size(); file_index++) {
		threads[file_index] = BlazingThread([&, file_index]() {
		  parquet_readers[file_index] =
			  std::move(parquet::ParquetFileReader::Open(files[file_index]));
		  std::shared_ptr<parquet::FileMetaData> file_metadata = parquet_readers[file_index]->metadata();
		  const parquet::SchemaDescriptor * schema = file_metadata->schema();
		  num_row_groups[file_index] = file_metadata->num_row_groups();
		});
	}

	for(int file_index = 0; file_index < files.size(); file_index++) {
		threads[file_index].join();
	}

	size_t total_num_row_groups =
		std::accumulate(num_row_groups.begin(), num_row_groups.end(), size_t(0));

	auto minmax_metadata_table = get_minmax_metadata(parquet_readers, total_num_row_groups, offset);
	for (auto &reader : parquet_readers) {
		reader->Close();
	}
	return std::move(minmax_metadata_table);
}

} /* namespace io */
} /* namespace ral */
