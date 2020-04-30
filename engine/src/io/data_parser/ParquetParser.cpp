
#include "metadata/parquet_metadata.h"

#include "ParquetParser.h"
#include "utilities/CommonOperations.h"

#include <numeric>

#include <arrow/io/file.h>
#include "blazingdb/concurrency/BlazingThread.h"

#include <parquet/column_writer.h>
#include <parquet/file_writer.h>

namespace ral {
namespace io {

namespace cudf_io = cudf::experimental::io;

parquet_parser::parquet_parser() {
	// TODO Auto-generated constructor stub
}

parquet_parser::~parquet_parser() {
	// TODO Auto-generated destructor stub
}

std::unique_ptr<ral::frame::BlazingTable> parquet_parser::parse(
	std::shared_ptr<arrow::io::RandomAccessFile> file,
	const Schema & schema,
	std::vector<size_t> column_indices) 
{
	
	if(file == nullptr) {
		return schema.makeEmptyBlazingTable(column_indices);
	}
		
	if(column_indices.size() > 0) {
		// Fill data to pq_args
		cudf_io::read_parquet_args pq_args{cudf_io::source_info{file}};

		pq_args.strings_to_categorical = false;
		pq_args.columns.resize(column_indices.size());

		for(size_t column_i = 0; column_i < column_indices.size(); column_i++) {
			pq_args.columns[column_i] = schema.get_name(column_indices[column_i]);
		}

		std::vector<int> row_groups = schema.get_rowgroup_ids(0); // because the Schema we are using here was already filtered for a specific file by Schema::fileSchema we are simply getting the first set of rowgroup_ids
		if (row_groups.size() == 0){
			// make empty table of the right schema
			return schema.makeEmptyBlazingTable(column_indices);
		} else {
			// now lets get these row_groups in batches of consecutive rowgroups because that is how the reader will want them
			std::vector<int> consecutive_row_group_start(1, row_groups[0]);
			std::vector<int> consecutive_row_group_length;
			int length_count = 1;
			int last_rowgroup = consecutive_row_group_start.back();
			for (int i = 1; i < row_groups.size(); i++){
				if (last_rowgroup + 1 == row_groups[i]){ // consecutive
					length_count++;
					last_rowgroup = row_groups[i];
				} else {
					consecutive_row_group_length.push_back(length_count);
					consecutive_row_group_start.push_back(row_groups[i]);
					last_rowgroup = row_groups[i];
					length_count = 1;
				}
			}
			consecutive_row_group_length.push_back(length_count);

			if (consecutive_row_group_start.size() == 1){
				pq_args.row_group = consecutive_row_group_start[0];
				pq_args.row_group_count = consecutive_row_group_length[0];

				auto result = cudf_io::read_parquet(pq_args);
				if (result.tbl->num_columns() == 0){
					return schema.makeEmptyBlazingTable(column_indices);
				}
				return std::make_unique<ral::frame::BlazingTable>(std::move(result.tbl), result.metadata.column_names);
			} else {
				std::vector<std::unique_ptr<ral::frame::BlazingTable>> table_outs;
				std::vector<ral::frame::BlazingTableView> table_view_outs;
				for (int i = 0; i < consecutive_row_group_start.size(); i++){
					pq_args.row_group = consecutive_row_group_start[i];
					pq_args.row_group_count = consecutive_row_group_length[i];

					auto result = cudf_io::read_parquet(pq_args);
					if (result.tbl->num_columns() == 0){
						return schema.makeEmptyBlazingTable(column_indices);
					}
					table_outs.emplace_back(std::make_unique<ral::frame::BlazingTable>(std::move(result.tbl), result.metadata.column_names));
					table_view_outs.emplace_back(table_outs.back()->toBlazingTableView());
				}
				return ral::utilities::experimental::concatTables(table_view_outs);
			}
		}
	}
	return nullptr;
}

std::unique_ptr<ral::frame::BlazingTable> parquet_parser::parse_batch(
	std::shared_ptr<arrow::io::RandomAccessFile> file,
	const Schema & schema,
	std::vector<size_t> column_indices,
	cudf::size_type row_group)
{
	if(file == nullptr) {
		return schema.makeEmptyBlazingTable(column_indices);
	}
	if(column_indices.size() > 0) {
		// Fill data to pq_args
		cudf_io::read_parquet_args pq_args{cudf_io::source_info{file}};

		pq_args.strings_to_categorical = false;
		pq_args.columns.resize(column_indices.size());

		for(size_t column_i = 0; column_i < column_indices.size(); column_i++) {
			pq_args.columns[column_i] = schema.get_name(column_indices[column_i]);
		}

		pq_args.row_group = row_group;

		auto result = cudf_io::read_parquet(pq_args);

		auto result_table = std::move(result.tbl);
		if (result.metadata.column_names.size() > column_indices.size()) {
			auto columns = result_table->release();
			// Assuming columns are in the same order as column_indices and any extra columns (i.e. index column) are put last
			columns.resize(column_indices.size());
			result_table = std::make_unique<cudf::experimental::table>(std::move(columns));
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

	cudf_io::read_parquet_args pq_args{cudf_io::source_info{file}};
	pq_args.strings_to_categorical = false;
	pq_args.row_group = 0;
	pq_args.num_rows = 1;

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
