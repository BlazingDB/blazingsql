/*
 * CodeTimer.cpp
 *
 *  Created on: Mar 31, 2016
 *      Author: root
 */

#include "CodeTimer.h"

#include <iostream>

namespace {
using blazingdb::manager::experimental::Context;
}  // namespace

CodeTimer::CodeTimer() { start = std::chrono::high_resolution_clock::now(); }

CodeTimer::~CodeTimer() {}

void CodeTimer::reset() { start = std::chrono::high_resolution_clock::now(); }

double CodeTimer::getDuration() {
	std::chrono::high_resolution_clock::time_point end = std::chrono::high_resolution_clock::now();
	return std::chrono::duration<double, std::milli>(end - start).count();
}

void CodeTimer::display() { display(""); }

void CodeTimer::display(std::string msg) {
	std::chrono::high_resolution_clock::time_point end = std::chrono::high_resolution_clock::now();
	double duration = std::chrono::duration<double, std::milli>(end - start).count();
	std::cout << "TIMING: " << msg << " | Duration: " << duration << "ms" << std::endl;
}

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
				   std::to_string(query_substep) + "|" + eventDescription + "|" + std::to_string(this->getDuration()) +
				   "|" + eventExtraInfo + "|" + std::to_string(measure) + "|" + eventExtraInfo2 + "|" +
				   std::to_string(measure2);
		else
			return std::to_string(contextToken) + "|" + std::to_string(query_step) + "|" +
				   std::to_string(query_substep) + "|" + eventDescription + "|" + std::to_string(this->getDuration()) +
				   "|" + eventExtraInfo + "|" + std::to_string(measure) + "||";
	} else
		return std::to_string(contextToken) + "|" + std::to_string(query_step) + "|" + std::to_string(query_substep) +
			   "|" + eventDescription + "|" + std::to_string(this->getDuration()) + "||||";
}