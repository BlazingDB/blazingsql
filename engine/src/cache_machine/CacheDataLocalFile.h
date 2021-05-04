#include "CacheData.h"

namespace ral {
namespace cache {

/**
* A CacheData that stores is data in an ORC file.
* This allows us to cache onto filesystems to allow larger queries to run on
* limited resources. This is the least performant cache in most instances.
*/
class CacheDataLocalFile : public CacheData {
public:

	/**
	* Constructor
	* @param table The BlazingTable that is converted into an ORC file and stored
	* on disk.
	* @ param orc_files_path The path where the file should be stored.
	* @ param ctx_id The context token to identify the query that generated the file.
	*/
	CacheDataLocalFile(std::unique_ptr<ral::frame::BlazingTable> table, std::string orc_files_path, std::string ctx_token);

	/**
	* Constructor
	* @param table The BlazingTable that is converted into an ORC file and stored
	* on disk.
	* @ param orc_files_path The path where the file should be stored.
	*/
	std::unique_ptr<ral::frame::BlazingTable> decache() override;

	/**
 	* Get the amount of GPU memory that the decached BlazingTable WOULD consume.
 	* Having this function allows us to have one api for seeing how much GPU
	* memory is necessary to decache the file from disk.
 	* @return The number of bytes needed for the BlazingTable decache would
	* generate.
 	*/
	size_t sizeInBytes() const override;
	/**
	* Get the amount of disk space consumed by this CacheData
	* Having this function allows us to have one api for seeing the consumption
	* of all the CacheData objects that are currently in Caches.
	* @return The number of bytes the ORC file consumes.
	*/
	size_t fileSizeInBytes() const;

	/**
	* Set the names of the columns to pass when decache if needed.
	* @param names a vector of the column names.
	*/
	void set_names(const std::vector<std::string> & names) override {
		this->col_names = names;
	}

	/**
	* Destructor
	*/
	virtual ~CacheDataLocalFile() {}

	/**
	* Get the file path of the ORC file.
	* @return The path to the ORC file.
	*/
	std::string filePath() const { return filePath_; }

private:
	std::vector<std::string> col_names; /**< The names of the columns, extracted from the ORC file. */
	std::string filePath_; /**< The path to the ORC file. Is usually generated randomly. */
	size_t size_in_bytes; /**< The size of the file being stored. */
};

} // namespace cache
} // namespace ral