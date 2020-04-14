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

protected:
	std::thread thread;
	std::shared_ptr<BlazingExceptionHolder> exceptionHolder;
};

namespace detail {
template <class F, class Tuple, std::size_t... I>
constexpr decltype(auto) apply_impl( F&& f, Tuple&& t, std::index_sequence<I...> )
{
  return f(std::get<I>(std::move<Tuple>(t))...);
}

template <class F, class Tuple>
constexpr decltype(auto) apply(F&& f, Tuple&& t)
{
    return detail::apply_impl(std::forward<F>(f), std::forward<Tuple>(t),
        std::make_index_sequence<std::tuple_size<std::decay_t<Tuple>>::value>());
}

} // namespace detail

class BlazingMutableThread : public BlazingThread {
public:
	template<class Func, class ...Args>
	explicit BlazingMutableThread(Func && func, Args &&... args)  
		: BlazingThread{}
	{
		auto holder = std::make_shared<BlazingExceptionHolder>();
		this->exceptionHolder = holder;
		auto tpl = std::make_tuple(std::forward<Args>(args)...);
		this->thread = std::thread([holder, 
									func = std::forward<Func>(func), 
									tpl = move(tpl)]
									() mutable {
			try {
				detail::apply(func, tpl);
			} catch(...) {
				holder->setException(std::current_exception());
			}
		});
	}
};

#endif /* BLAZINGTHREAD_H_ */
