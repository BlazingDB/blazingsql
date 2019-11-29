#ifndef BLAZINGDB_RAL_EXCEPTION_RALEXCEPTION_H
#define BLAZINGDB_RAL_EXCEPTION_RALEXCEPTION_H

#include <string>

namespace ral {
namespace exception {

class RalException {
public:
    virtual ~RalException() = default;

public:
    virtual const std::string& what() const = 0;
};

class BaseRalException : public RalException {
public:
    BaseRalException(std::string&& message);

    BaseRalException(const std::string& message);

public:
    const std::string& what() const override;

private:
    const std::string message_;
};

} // namespace exception
} // namespace ral

#endif //BLAZINGDB_RAL_EXCEPTION_RALEXCEPTION_H
