#include "distribution/Exception.h"

namespace ral {
namespace distribution {

MessageMismatchException::MessageMismatchException(std::string && message) : BaseRalException{std::move(message)} {}

MessageMismatchException createMessageMismatchException(
	const char * function, const std::string & expected, const std::string & obtained) {
	std::string message{"[ERROR][" + std::string{function} + "][expected:" + expected + "][obtained:" + obtained + "]"};
	return MessageMismatchException(std::move(message));
}

}  // namespace distribution
}  // namespace ral
