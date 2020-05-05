#include "CodeTimer.h"

namespace {
using blazingdb::manager::experimental::Context;
}  // namespace

std::string CodeTimer::logDuration(const Context & context,
	std::string eventDescription,
	std::string eventExtraInfo,
	int measure,
	std::string eventExtraInfo2,
	int measure2) {
	return logDuration(context.getContextToken(),
		context.getQueryStep(),
		context.getQuerySubstep(),
		eventDescription,
		eventExtraInfo,
		measure,
		eventExtraInfo2,
		measure2);
}

std::string CodeTimer::logDuration(uint32_t contextToken,
	uint32_t query_step,
	uint32_t query_substep,
	std::string eventDescription,
	std::string eventExtraInfo,
	int measure,
	std::string eventExtraInfo2,
	int measure2) {
	if(eventExtraInfo != "") {
		if(eventExtraInfo2 != "")
			return std::to_string(contextToken) + "|" + std::to_string(query_step) + "|" +
				   std::to_string(query_substep) + "|" + eventDescription + "|" + std::to_string(this->elapsed_time()) +
				   "|" + eventExtraInfo + "|" + std::to_string(measure) + "|" + eventExtraInfo2 + "|" +
				   std::to_string(measure2);
		else
			return std::to_string(contextToken) + "|" + std::to_string(query_step) + "|" +
				   std::to_string(query_substep) + "|" + eventDescription + "|" + std::to_string(this->elapsed_time()) +
				   "|" + eventExtraInfo + "|" + std::to_string(measure) + "||";
	} else
		return std::to_string(contextToken) + "|" + std::to_string(query_step) + "|" + std::to_string(query_substep) +
			   "|" + eventDescription + "|" + std::to_string(this->elapsed_time()) + "||||";
}
