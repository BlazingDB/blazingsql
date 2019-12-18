#include "exception/RalException.h"

namespace ral {
namespace exception {

BaseRalException::BaseRalException(std::string && message) : message_{std::move(message)} {}

BaseRalException::BaseRalException(const std::string & message) : message_{message} {}

const std::string & BaseRalException::what() const { return message_; }

}  // namespace exception
}  // namespace ral
