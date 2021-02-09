#pragma once

#include <chrono>

// Based on https://github.com/andremaravilha/cxxtimer
class CodeTimer {
	using Clock = std::chrono::high_resolution_clock;

public:
	CodeTimer(bool start = true)
		: started_{false}, paused_{false}, start_point_{Clock::now()}, accumulated_{Clock::duration(0)}, end_point_{} {
		if(start) {
			this->start();
		}
	}

	~CodeTimer() = default;

	void start() {
		if(!started_) {
			started_ = true;
			paused_ = false;
			accumulated_ = Clock::duration(0);
			start_point_ = Clock::now();
		} else if(paused_) {
			start_point_ = Clock::now();
			paused_ = false;
		}
	}

	void stop() {
		if(started_ && !paused_) {
			Clock::time_point now = end_point_ = Clock::now();
			accumulated_ += now - start_point_;
			paused_ = true;
		}
	}

	void reset() {
		if(started_) {
			started_ = false;
			paused_ = false;
			start_point_ = Clock::now();
			accumulated_ = Clock::duration(0);
		}
	}

	template <typename Units = std::chrono::milliseconds>
	typename Units::rep start_time() {
		return std::chrono::duration_cast<Units>(start_point_.time_since_epoch()).count();
	}

	template <typename Units = std::chrono::milliseconds>
	typename Units::rep end_time() {
		// if we are getting the end_time() but its still where it was initialized, then we want to get the time now.
		if (end_point_ == Clock::time_point{}){
			end_point_ = Clock::now();
		}
		return std::chrono::duration_cast<Units>(end_point_.time_since_epoch()).count();
	}

	template <typename Units = std::chrono::milliseconds>
	typename Units::rep elapsed_time() {
		if(started_) {
			if(paused_) {
				return std::chrono::duration_cast<Units>(accumulated_).count();
			} else {
				end_point_ = Clock::now();
				return std::chrono::duration_cast<Units>(accumulated_ + (end_point_ - start_point_)).count();
			}
		} else {
			return Clock::duration(0).count();
		}
	}

private:
	bool started_;
	bool paused_;
	Clock::time_point start_point_;
	Clock::time_point end_point_;
	Clock::duration accumulated_;
};
