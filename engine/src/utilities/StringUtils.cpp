#include "StringUtils.h"

std::string ral::utilities::buildLogString(std::string arg1,
	std::string arg2,
	std::string arg3,
	std::string arg4,
	std::string arg5,
	std::string arg6,
	std::string arg7,
	std::string arg8,
	std::string arg9) {
	return arg1 + "|" + arg2 + "|" + arg3 + "|" + arg4 + "|" + arg5 + "|" + arg6 + "|" + arg7 + "|" + arg8 + "|" + arg9;
}
