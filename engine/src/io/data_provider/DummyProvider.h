/*
 * uridataprovider.h
 *
 *  Created on: Nov 29, 2018
 *      Author: felipe
 */

#ifndef DUMMYPROVIDER_H_
#define DUMMYPROVIDER_H_

#include "DataProvider.h"
#include "FileSystem/Uri.h"
#include <arrow/io/interfaces.h>
#include <memory>
#include <vector>


namespace ral {
namespace io {

class dummy_data_provider : public data_provider {
public:
	dummy_data_provider() {}

	std::shared_ptr<data_provider> clone() override {
		return std::make_shared<dummy_data_provider>();
	}

	virtual ~dummy_data_provider() {}

	bool has_next() { return false; }

	void reset() {
		// does nothing
	}

	data_handle get_next(bool open_file = true) {
		data_handle handle;
		handle.fileHandle = nullptr;
		return handle;
	}

	std::vector<std::string> get_errors() { return {}; }

	/**
	 * Tries to get up to num_files data_handles. We use this instead of a get_all() because if there are too many files, 
	 * trying to get too many file handles will cause a crash. Using get_some() forces breaking up the process of getting file_handles.
	 */
	std::vector<data_handle> get_some(std::size_t num_files, bool open_file = true) { return {}; }

	/**
	 * Closes currently open set of file handles maintained by the provider
	 */
	void close_file_handles() {
		// does nothing
	}
	

private:
};

} /* namespace io */
} /* namespace ral */

#endif /* DUMMYPROVIDER_H_ */
