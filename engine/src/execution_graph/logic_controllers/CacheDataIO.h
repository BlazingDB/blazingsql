#include "CacheData.h"

namespace ral {
namespace cache {

/**
* A CacheData that stores is data in an ORC file.
* This allows us to cache onto filesystems to allow larger queries to run on
* limited resources. This is the least performant cache in most instances.
*/
class CacheDataIO : public CacheData {
public:

	/**
	* Constructor
	* @param table The BlazingTable that is converted into an ORC file and stored
	* on disk.
	* @ param orc_files_path The path where the file should be stored.
	*/
	CacheDataIO(ral::io::data_handle handle,
		std::shared_ptr<ral::io::data_parser> parser,
		ral::io::Schema schema,
		ral::io::Schema file_schema,
		std::vector<int> row_group_ids,
		std::vector<int> projections
	);

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
	* Set the names of the columns from the schema.
	* @param names a vector of the column names.
	*/
	void set_names(const std::vector<std::string> & names) override;

	/**
	* Destructor
	*/
	virtual ~CacheDataIO() {}

private:
	ral::io::data_handle handle;
	std::shared_ptr<ral::io::data_parser> parser;
	ral::io::Schema schema;
	ral::io::Schema file_schema;
	std::vector<int> row_group_ids;
	std::vector<int> projections;
};

} // namespace cache
} // namespace ral