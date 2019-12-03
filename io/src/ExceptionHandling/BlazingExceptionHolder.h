#ifndef BLAZINGEXCEPTIONHOLDER_H_
#define BLAZINGEXCEPTIONHOLDER_H_

#include <exception>

class BlazingExceptionHolder {
public:
	BlazingExceptionHolder();

	~BlazingExceptionHolder();

public:
	BlazingExceptionHolder(BlazingExceptionHolder &&) = delete;

	BlazingExceptionHolder(const BlazingExceptionHolder &) = delete;

	BlazingExceptionHolder & operator=(BlazingExceptionHolder &&) = delete;

	BlazingExceptionHolder & operator=(const BlazingExceptionHolder &) = delete;

public:
	bool hasDetached();

	bool hasCompleted();

public:
	void setDetached(bool value);

	void setCompleted(bool value);

public:
	bool hasException();

	void throwException();

	void setException(std::exception_ptr && value);

private:
	bool detached;
	bool completed;

private:
	std::exception_ptr exception;
};

#endif
