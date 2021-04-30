#include "CacheDataLocalFile.h"
#include <random>
#include "cudf/types.hpp" //cudf::io::metadata
#include <cudf/io/orc.hpp>

namespace ral {
namespace cache {

//TODO: Rommel Use randomeString from StringUtil
std::string randomString(std::size_t length) {
	const std::string characters = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";

	std::random_device random_device;
	std::mt19937 generator(random_device());
	std::uniform_int_distribution<> distribution(0, characters.size() - 1);

	std::string random_string;

	for(std::size_t i = 0; i < length; ++i) {
		random_string += characters[distribution(generator)];
	}

	return random_string;
}

CacheDataLocalFile::CacheDataLocalFile(std::unique_ptr<ral::frame::BlazingTable> table, std::string orc_files_path, std::string ctx_token)
	: CacheData(CacheDataType::LOCAL_FILE, table->names(), table->get_schema(), table->num_rows())
{
	this->size_in_bytes = table->sizeInBytes();
	this->filePath_ = orc_files_path + "/.blazing-temp-" + ctx_token + "-" + randomString(64) + ".orc";

	// filling this->col_names
	for(auto name : table->names()) {
		this->col_names.push_back(name);
	}

	int attempts = 0;
	int attempts_limit = 10;
	while(attempts <= attempts_limit){
		try {
			cudf::io::table_metadata metadata;
			for(auto name : table->names()) {
				metadata.column_names.emplace_back(name);
			}

			cudf::io::orc_writer_options out_opts = cudf::io::orc_writer_options::builder(cudf::io::sink_info{this->filePath_}, table->view())
				.metadata(&metadata);

			cudf::io::write_orc(out_opts);
		} catch (cudf::logic_error & err){
			std::shared_ptr<spdlog::logger> logger = spdlog::get("batch_logger");
			if(logger) {
				logger->error("|||{info}||||rows|{rows}",
					"info"_a="Failed to create CacheDataLocalFile in path: " + this->filePath_ + " attempt " + std::to_string(attempts),
					"rows"_a=table->num_rows());
			}	
			attempts++;
			if (attempts == attempts_limit){
				throw;
			}
			std::this_thread::sleep_for (std::chrono::milliseconds(5 * attempts));
		}
	}
}

size_t CacheDataLocalFile::fileSizeInBytes() const {
	struct stat st;

	if(stat(this->filePath_.c_str(), &st) == 0)
		return (st.st_size);
	else
		throw;
}

size_t CacheDataLocalFile::sizeInBytes() const {
	return size_in_bytes;
}

std::unique_ptr<ral::frame::BlazingTable> CacheDataLocalFile::decache() {

	cudf::io::orc_reader_options read_opts = cudf::io::orc_reader_options::builder(cudf::io::source_info{this->filePath_});
	auto result = cudf::io::read_orc(read_opts);

	// Remove temp orc files
	const char *orc_path_file = this->filePath_.c_str();
	remove(orc_path_file);
	return std::make_unique<ral::frame::BlazingTable>(std::move(result.tbl), this->col_names );
}

} // namespace cache
} // namespace ral