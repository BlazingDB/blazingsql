/*
 * BlazingThread.h
 *
 *  Created on: Feb 9, 2018
 *      Author: felipe
 */

#ifndef BLAZINGTHREAD_H_
#define BLAZINGTHREAD_H_

#include "BlazingExceptionHolder.h"
#include <thread>
#include <functional>

class BlazingThread {
public:
	template <class... Args>
	explicit BlazingThread(Args &&... args) {
		auto holder = std::make_shared<BlazingExceptionHolder>();
		exceptionHolder = holder;

		thread = std::thread([holder, args...]() {
			try {
				std::bind(args...)();
			} catch(...) {
				holder->setException(std::current_exception());
			}
		});
	}

	BlazingThread();

	virtual ~BlazingThread();

public:
	BlazingThread(BlazingThread && other) noexcept;

	BlazingThread & operator=(BlazingThread && other) noexcept;

	BlazingThread(const BlazingThread & other) = delete;

	BlazingThread & operator=(const BlazingThread &) = delete;

public:
	void join();
	void detach();

	static unsigned int hardware_concurrency() { return std::thread::hardware_concurrency(); }

public:
	bool hasException();

	void throwException();

private:
	std::thread thread;
	std::shared_ptr<BlazingExceptionHolder> exceptionHolder;
};

#endif /* BLAZINGTHREAD_H_ */
