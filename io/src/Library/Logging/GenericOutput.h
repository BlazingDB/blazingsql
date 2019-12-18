#ifndef SRC_LIBRARY_LOGGING_GENERICOUTPUT_H_
#define SRC_LIBRARY_LOGGING_GENERICOUTPUT_H_

#include <string>

namespace Library {
namespace Logging {
class GenericOutput {
public:
	virtual ~GenericOutput() {}

	virtual void flush(std::string && log) = 0;

	virtual void flush(const std::string & log) = 0;

	virtual void flush(
		const int nodeInd, const std::string & datetime, const std::string & level, const std::string & log) = 0;

	// virtual void setNodeIdentifier(const unsigned int nodeInd) = 0;
};
}  // namespace Logging
}  // namespace Library

#endif
