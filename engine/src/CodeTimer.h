/*
 * CodeTimer.h
 *
 *  Created on: Mar 31, 2016
 *      Author: root
 */

#ifndef CODETIMER_H_
#define CODETIMER_H_

#include <blazingdb/manager/Context.h>
#include <chrono>
#include <string>

namespace {
using blazingdb::manager::experimental::Context;
}  // namespace

class CodeTimer {
public:
	CodeTimer();
	virtual ~CodeTimer();
	void reset();
	void display();
	double getDuration();
	void display(std::string msg);

	std::string logDuration(const Context & context,
		std::string eventDescription,
		std::string eventExtraInfo = "",
		int measure = 0,
		std::string eventExtraInfo2 = "",
		int measure2 = 0);
	std::string logDuration(uint32_t contextToken,
		uint32_t query_step,
		uint32_t query_substep,
		std::string eventDescription,
		std::string eventExtraInfo = "",
		int measure = 0,
		std::string eventExtraInfo2 = "",
		int measure2 = 0);


private:
	std::chrono::high_resolution_clock::time_point start;
};

#endif /* CODETIMER_H_ */