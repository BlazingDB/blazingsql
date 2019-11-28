/*
 * FileReaderInterface.h
 *
 *  Created on: Sep 28, 2018
 *      Author: felipe
 */

#ifndef FILEREADERINTERFACE_H_
#define FILEREADERINTERFACE_H_

namespace blazing {
namespace io {

template <typename FileSystem, typename ReadableFileType>
class FileReaderInterface {
public:
	FileReaderInterface();
	virtual ~FileReaderInterface();
	virtual void openFile() = 0;
	virtual void closeFile() = 0;
	virtual
};

} /* namespace io */
} /* namespace blazing */

#endif /* FILEREADERINTERFACE_H_ */
