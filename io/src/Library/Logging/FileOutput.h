#ifndef SRC_LIBRARY_LOGGING_FILEOUTPUT_H_
#define SRC_LIBRARY_LOGGING_FILEOUTPUT_H_

#include "Library/Logging/GenericOutput.h"
#include <fstream>
#include <mutex>

namespace Library {
namespace Logging {
class FileOutput : public GenericOutput {
public:
	FileOutput(const std::string & filename, bool truncate = false);

	~FileOutput();

public:
	FileOutput(FileOutput &&) = delete;

	FileOutput(const FileOutput &) = delete;

	FileOutput & operator=(FileOutput &&) = delete;

	FileOutput & operator=(const FileOutput &) = delete;

public:
	void flush(std::string && log) override;

	void flush(const std::string & log) override;

	void flush(
		const int nodeInd, const std::string & datetime, const std::string & level, const std::string & log) override;

	// void setNodeIdentifier(const unsigned int nodeInd) override;

private:
	std::mutex mutex;
	std::ofstream file;
};
}  // namespace Logging
}  // namespace Library

#endif
