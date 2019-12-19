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

	virtual ~dummy_data_provider() {}

	bool has_next() { return false; }

	void reset() {
		// does nothing
	}

	data_handle get_next() {
		data_handle handle;
		handle.fileHandle = nullptr;
		return handle;
	}

	data_handle get_first() {
		data_handle handle;
		handle.fileHandle = nullptr;
		return handle;
	}


	std::vector<std::string> get_errors() { return {}; }

	std::string get_current_user_readable_file_handle() { return ""; }

	std::vector<data_handle> get_all() { return {}; }


	size_t get_file_index() { return 0; }

	std::vector<data_handle> * data_handles() const noexcept final /* __attribute__((noreturn)) */ {
		// NOTE: You shouldn't have arrived here. This is only valid for queries from files.
		// You must be validate you are no calling this function when there is not data_handles container
		// throw std::runtime_error("no data handles in dummy");
		return nullptr;
	}

	void step() noexcept final{};

private:
};

} /* namespace io */
} /* namespace ral */

#endif /* DUMMYPROVIDER_H_ */
