#ifndef BLAZINGDB_RAL_DISTRIBUTION_EXCEPTION_H
#define BLAZINGDB_RAL_DISTRIBUTION_EXCEPTION_H

#include "exception/RalException.h"

namespace ral {
namespace distribution {

class MessageMismatchException : public ral::exception::BaseRalException {
public:
    MessageMismatchException(std::string&& message);
};

MessageMismatchException createMessageMismatchException(const char* function,
                                                        const std::string& expected,
                                                        const std::string& obtained);

} // namespace distribution
} // namespace ral

#endif //BLAZINGDB_RAL_DISTRIBUTION_EXCEPTION_H
