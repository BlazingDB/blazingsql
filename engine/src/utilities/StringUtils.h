#ifndef _BLAZINGDB_RAL_STRING_UTILS_COMMON_H
#define _BLAZINGDB_RAL_STRING_UTILS_COMMON_H

#include <string>

namespace ral {
namespace utilities {

std::string buildLogString(std::string arg1 = "",
	std::string arg2 = "",
	std::string arg3 = "",
	std::string arg4 = "",
	std::string arg5 = "",
	std::string arg6 = "",
	std::string arg7 = "",
	std::string arg8 = "",
	std::string arg9 = "");

}  // namespace utilities
}  // namespace ral

#endif  //_BLAZINGDB_RAL_STRING_UTILS_COMMON_H
