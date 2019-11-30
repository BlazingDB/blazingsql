/*
 * BlazingThread.cpp
 *
 *  Created on: Feb 9, 2018
 *      Author: felipe
 */

#include "BlazingThread.h"

BlazingThread::BlazingThread() {}

BlazingThread::~BlazingThread() {
	if(this->thread.joinable()) {
		this->thread.detach();
	}
}

BlazingThread::BlazingThread(BlazingThread && other) noexcept {
	thread = std::move(other.thread);
	exceptionHolder = std::move(other.exceptionHolder);
}

BlazingThread & BlazingThread::operator=(BlazingThread && other) noexcept {
	if(this != &other) {
		thread = std::move(other.thread);
		exceptionHolder = std::move(other.exceptionHolder);
	}

	return *this;
}

void BlazingThread::join() {
	thread.join();
	throwException();
}

void BlazingThread::detach() {
	this->thread.detach();

	if(this->exceptionHolder) {
		if(this->exceptionHolder->hasCompleted()) {
			//				delete this->exceptionHolder;
		} else {
			this->exceptionHolder->setDetached(true);
		}
	}
}


bool BlazingThread::hasException() {
	if(exceptionHolder == nullptr) {
		return false;
	}
	return exceptionHolder->hasException();
}

void BlazingThread::throwException() {
	if(exceptionHolder && (exceptionHolder->hasException())) {
		exceptionHolder->throwException();
	}
}
