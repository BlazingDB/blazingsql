#include "BlazingExceptionHolder.h"
#include <utility>

BlazingExceptionHolder::BlazingExceptionHolder() : detached{false}, completed{false}, exception{nullptr} {}

BlazingExceptionHolder::~BlazingExceptionHolder() {}

bool BlazingExceptionHolder::hasDetached() { return detached; }

bool BlazingExceptionHolder::hasCompleted() { return completed; }

void BlazingExceptionHolder::setDetached(bool value) { detached = value; }

void BlazingExceptionHolder::setCompleted(bool value) { completed = value; }

bool BlazingExceptionHolder::hasException() { return (exception != nullptr); }

void BlazingExceptionHolder::throwException() {
	if(exception != nullptr) {
		std::exception_ptr temp = exception;
		exception = nullptr;
		std::rethrow_exception(temp);
	}
}

void BlazingExceptionHolder::setException(std::exception_ptr && value) { exception = std::move(value); }
