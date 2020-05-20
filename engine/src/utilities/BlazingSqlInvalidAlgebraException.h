#ifndef BLAZING_SQL_INVALID_ALGEBRA_EXCEPTION_H
#define BLAZING_SQL_INVALID_ALGEBRA_EXCEPTION_H

#include <exception>
#include <string>

namespace ral {
namespace utilities {

class BlazingSqlInvalidAlgebraException : public std::exception
{
public:
    BlazingSqlInvalidAlgebraException(const std::string& msg) : exception_msg(msg){}

    virtual const char* what() const noexcept override
    {
        return exception_msg.c_str();
    }

private:
	std::string exception_msg;
}; 

}  // namespace utilities
}  // namespace ral

#endif  // BLAZING_SQL_INVALID_ALGEBRA_EXCEPTION_H
