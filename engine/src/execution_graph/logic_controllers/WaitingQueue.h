#pragma once

#include <atomic>
#include <deque>
#include <memory>
#include <condition_variable>
#include <mutex>
#include <string>
#include <vector>
#include <map>

#include <spdlog/spdlog.h>
#include <exception>

using namespace std::chrono_literals;

namespace ral {
namespace cache {

using Context = blazingdb::manager::Context;
using namespace fmt::literals;



/**
* A Queue that has built in methods for waiting operations.
* The purpose of WaitingQueue is to provide apis that get things out of the
* queue when they exist and wait for things when they don't without consuming
* many compute resources.This is accomplished through the use of a
* condition_variable and mutex locks.
*/
template <typename message_ptr>
class WaitingQueue {
public:

	/**
	* Constructor
	*/
	WaitingQueue(std::string queue_name, int timeout = 60000, bool log_timeout = true) : 
		queue_name(queue_name), finished{false}, timeout(timeout), log_timeout(log_timeout) {}

	/**
	* Destructor
	*/
	~WaitingQueue() = default;

	WaitingQueue(WaitingQueue &&) = delete;
	WaitingQueue(const WaitingQueue &) = delete;
	WaitingQueue & operator=(WaitingQueue &&) = delete;
	WaitingQueue & operator=(const WaitingQueue &) = delete;

	/**
	* Put a message onto the WaitingQueue using unique_lock.
	* This message aquires a unique_lock and then pushes a message onto the
	* WaitingQueue. It then increments the processed count and notifies the
	* WaitingQueue's condition variable.
	* @param item the message_ptr being added to the WaitingQueue
	*/
	void put(message_ptr item) {
		std::unique_lock<std::mutex> lock(mutex_);
		putWaitingQueue(std::move(item));
		processed++;
		condition_variable_.notify_all();
	}

	/**
	* Get number of partitions processed.
	* @return number of partitions that have been inserted into this WaitingQueue.
	*/
	int processed_parts(){
		std::unique_lock<std::mutex> lock(mutex_);
		return processed;
	}

	/**
	* Finish lets us know that know more messages will come in.
	* This exists so that if anyone is trying to pull from a queue that is already
	* completed that operation will return nullptr so it will not block
	* indefinitely.
	*/
	void finish() {
		std::unique_lock<std::mutex> lock(mutex_);
		this->finished = true;
		condition_variable_.notify_all();
	}

	/**
	* Lets us know if a WaitingQueue has finished running messages.
	* @return A bool indicating whether or not this WaitingQueue is finished.
	* receiving messages.
	*/
	bool is_finished() {
		return this->finished.load(std::memory_order_seq_cst);
	}

	/**
	* Blocks executing thread until a certain number messages are reached.
	* We often want to block a thread from proceeding until a certain number ouf
	* messages exist in the WaitingQueue. It also alerts us if we ever receive
	* more messages than we expected.
	*/

	void wait_for_count(int count){

		CodeTimer blazing_timer;
		std::unique_lock<std::mutex> lock(mutex_);
		while(!condition_variable_.wait_for(lock, timeout*1ms, [&, this] {
				bool done_waiting = count == this->processed;
				if (!done_waiting && blazing_timer.elapsed_time() > 59000 && this->log_timeout){
                    std::shared_ptr<spdlog::logger> logger = spdlog::get("batch_logger");
					if(logger) {
						logger->warn("|||{info}|{duration}||||",
											"info"_a="WaitingQueue " + this->queue_name + " wait_for_count timed out. count = " + std::to_string(count) + " processed = " + std::to_string(this->processed),
											"duration"_a=blazing_timer.elapsed_time());
					}
				}
				if (count < this->processed){
					throw std::runtime_error("WaitingQueue::wait_for_count " + this->queue_name + " encountered " + std::to_string(this->processed) + " when expecting " + std::to_string(count));
				}
				return done_waiting;
			})){}


		// condition_variable_.wait(lock, [&, this] () {
		// 	if (count < this->processed){
		// 		throw std::runtime_error("WaitingQueue::wait_for_count " + this->queue_name + " encountered " + std::to_string(this->processed) + " when expecting " + std::to_string(count));
		// 	}
		// 	return count == this->processed;
		// });
	}

	/**
	* Get a message_ptr if it exists in the WaitingQueue else wait.
	* This function allows kernels to pull from the cache before a cache has
	* CacheData in it. If finish is called on the WaitingQueue and no messages
	* are left this returns nullptr.
	* @return A message_ptr that was pushed into the WaitingQueue nullptr if
	* the WaitingQueue is empty and finished.
	*/
	message_ptr pop_or_wait() {

		CodeTimer blazing_timer;
		std::unique_lock<std::mutex> lock(mutex_);
		while(!condition_variable_.wait_for(lock, timeout*1ms, [&, this] {
				bool done_waiting = this->finished.load(std::memory_order_seq_cst) or !this->empty();
				if (!done_waiting && blazing_timer.elapsed_time() > 59000 && this->log_timeout){
                    std::shared_ptr<spdlog::logger> logger = spdlog::get("batch_logger");
					if(logger) {
						logger->warn("|||{info}|{duration}||||",
											"info"_a="WaitingQueue " + this->queue_name + " pop_or_wait timed out",
											"duration"_a=blazing_timer.elapsed_time());
					}
				}
				return done_waiting;
			})){}

		// condition_variable_.wait(lock,[&, this] {
		// 		return this->finished.load(std::memory_order_seq_cst) or !this->empty();
		// });
		if(this->message_queue_.size() == 0) {
			return nullptr;
		}
		auto data = std::move(this->message_queue_.front());
		this->message_queue_.pop_front();
		return std::move(data);
	}

	/**
	* Get a message_ptr from the back of the queue if it exists in the WaitingQueue else return nullptr.
	* @return message_ptr from the back of the queue if it exists in the WaitingQueue else return nullptr.
	*/
	message_ptr pop_back() {

		std::lock_guard<std::mutex> lock(mutex_);
		if(this->message_queue_.size() == 0) {
			return nullptr;
		}
		auto data = std::move(this->message_queue_.back());
		this->message_queue_.pop_back();
		return std::move(data);
	}

	/**
	* Wait for the next message to be ready.
	* @return Waits for the next CacheData to be available. Returns true when this
	* is the case. Returns false if the WaitingQueue is both finished and empty.
	*/
	bool wait_for_next() {
		CodeTimer blazing_timer;
		std::unique_lock<std::mutex> lock(mutex_);
		while(!condition_variable_.wait_for(lock, timeout*1ms, [&, this] {
				bool done_waiting = this->finished.load(std::memory_order_seq_cst) or !this->empty();
				if (!done_waiting && blazing_timer.elapsed_time() > 59000 && this->log_timeout){
                    std::shared_ptr<spdlog::logger> logger = spdlog::get("batch_logger");
					if(logger) {
						logger->warn("|||{info}|{duration}||||",
											"info"_a="WaitingQueue " + this->queue_name + " wait_for_next timed out",
											"duration"_a=blazing_timer.elapsed_time());
					}
				}
				return done_waiting;
			})){}

		if(this->empty()) {
			return false;
		}
		return true;
	}
	/**
	* Indicates if the WaitingQueue has messages at this point in time.
	* @return A bool which is true if the WaitingQueue is not empty.
	*/
	bool has_next_now() {
		std::unique_lock<std::mutex> lock(mutex_);
		return !this->empty();
	}

	/**
	* Pauses a threads execution until this WaitingQueue has finished processing.
	* Sometimes, like in the case of Joins, we might be waiting for a
	* WaitingQueue to have finished before the next kernel can use the data it
	* contains.
	*/
	void wait_until_finished() {
		CodeTimer blazing_timer;
		std::unique_lock<std::mutex> lock(mutex_);
		while(!condition_variable_.wait_for(lock, timeout*1ms, [&blazing_timer, this] {
				bool done_waiting = this->finished.load(std::memory_order_seq_cst);
				if (!done_waiting && blazing_timer.elapsed_time() > 59000 && this->log_timeout){
                    std::shared_ptr<spdlog::logger> logger = spdlog::get("batch_logger");
					if(logger) {
					   logger->warn("|||{info}|{duration}||||",
										   "info"_a="WaitingQueue " + this->queue_name + " wait_until_finished timed out",
 										   "duration"_a=blazing_timer.elapsed_time());
					}
				}
				return done_waiting;
			})){}
	}

	/**
	* Waits until a certain number of bytes exist in the WaitingQueue
	* During some execution kernels it is better to wait for a certain amount
	* of the total anticipated data to be available before processing the next
	* batch.
	* @param num_bytes The number of bytes that we will wait to exist in the
	* WaitingQueue unless the WaitingQueue has already had finished() called.
	* @param num_bytes_timeout A timeout in ms where if there is data and the timeout 
	* expires, then it will return, even if not the requested num_bytes is available.
	* If its set to -1, then it disables this timeout
	*/
	void wait_until_num_bytes(size_t num_bytes, int num_bytes_timeout = -1) {
		CodeTimer blazing_timer;
		std::unique_lock<std::mutex> lock(mutex_);
		int cond_var_timeout = (num_bytes_timeout > -1 && num_bytes_timeout < timeout) ? num_bytes_timeout : timeout;
		while(!condition_variable_.wait_for(lock, cond_var_timeout*1ms, [&blazing_timer, num_bytes, num_bytes_timeout, this] {
				bool done_waiting = this->finished.load(std::memory_order_seq_cst);
				size_t total_bytes = 0;
				if (!done_waiting) {					
					for (auto & message : message_queue_){
						total_bytes += message->get_data().sizeInBytes();
					}
					done_waiting = total_bytes > num_bytes;
				}
				if (!done_waiting && total_bytes > 0 && num_bytes_timeout > -1) {
					done_waiting = blazing_timer.elapsed_time() > num_bytes_timeout;
				}
				if (!done_waiting && blazing_timer.elapsed_time() > 59000 && this->log_timeout){
                    std::shared_ptr<spdlog::logger> logger = spdlog::get("batch_logger");
					if(logger) {
						logger->warn("|||{info}|{duration}||||",
											"info"_a="WaitingQueue " + this->queue_name + " wait_until_num_bytes timed out num_bytes wanted: " + std::to_string(num_bytes) + " total_bytes: " + std::to_string(total_bytes),
											"duration"_a=blazing_timer.elapsed_time());
					}
				}
				return done_waiting;
			})){}
	}

	/**
	* Let's us know the size of the next CacheData to be pulled.
	* Sometimes it is useful to know how much data we will be pulling in each
	* CacheData that we have accumulated for making estimates on how much more
	* is going to be coming or seeing how far along we are.
	* @return The number of bytes consumed by the next message.
	*/
	size_t get_next_size_in_bytes(){
		std::unique_lock<std::mutex> lock(mutex_);
		if (message_queue_.size() > 0){
			return message_queue_[0]->get_data().sizeInBytes();
		} else {
			return 0;
		}
	}

	/**
	* Get a specific message from the WaitingQueue.
	* Messages are always accompanied by a message_id though in some cases that
	* id is empty string. This allows us to get a specific message from the
	* WaitingQueue and wait around for it to exist or for this WaitingQueue to be
	* finished. If WaitingQueue is finished and there are no messages we get
	* a nullptr.
	* @param message_id The id of the message that we want to get or wait for.
	* @return The message that has this id or nullptr if that message will  never
	* be able to arrive because the WaitingQueue is finished.
	*/
	message_ptr get_or_wait(std::string message_id) {
		CodeTimer blazing_timer;
		std::unique_lock<std::mutex> lock(mutex_);
		while(!condition_variable_.wait_for(lock, timeout*1ms, [message_id, &blazing_timer, this] {
				auto result = std::any_of(this->message_queue_.cbegin(),
							this->message_queue_.cend(), [&](auto &e) {
								return e->get_message_id() == message_id;
							});
				bool done_waiting = this->finished.load(std::memory_order_seq_cst) or result;
				if (!done_waiting && blazing_timer.elapsed_time() > 59000 && this->log_timeout){
                    std::shared_ptr<spdlog::logger> logger = spdlog::get("batch_logger");
					if(logger) {
						logger->warn("|||{info}|{duration}|message_id|{message_id}||",
											"info"_a="WaitingQueue " + this->queue_name + " get_or_wait timed out",
											"duration"_a=blazing_timer.elapsed_time(),
											"message_id"_a=message_id);
					}
				}
				return done_waiting;
			})){

			}
		if(this->message_queue_.size() == 0) {
			return nullptr;
		}
		while (true){
			auto data = this->pop_unsafe();
			if (data->get_message_id() == message_id){
				return std::move(data);
			} else {
				putWaitingQueue(std::move(data));
			}
		}
	}

	/**
	* Pop the front element WITHOUT thread safety.
	* Allos us to pop from the front in situations where we have already acquired
	* a unique_lock on this WaitingQueue's mutex.
	* @return The first message in the WaitingQueue.
	*/
	message_ptr pop_unsafe() {
		if(this->message_queue_.size() == 0) {
			return nullptr;
		}
		auto data = std::move(this->message_queue_.front());
		this->message_queue_.pop_front();
		return std::move(data);
	}

	/**
	 * gets all the messages
	 */
	std::vector<message_ptr> get_all(){
		std::unique_lock<std::mutex> lock(mutex_);
		return get_all_unsafe();
	}

	/**
	 * gets all the message ids
	 */
	std::vector<std::string> get_all_message_ids(){
		std::unique_lock<std::mutex> lock(mutex_);
		std::vector<std::string> message_ids;
		message_ids.reserve(message_queue_.size());
		for(message_ptr & it : message_queue_) {
			message_ids.push_back(it->get_message_id());
		}
		return message_ids;
	}

	/**
	* Waits until all messages are ready then returns all of them.
	* You should never call this function more than once on a WaitingQueue else
	* race conditions can occur.
	* @return A vector of all the messages that were inserted into the
	* WaitingQueue.
	*/
	std::vector<message_ptr> get_all_or_wait() {
		CodeTimer blazing_timer;
		std::unique_lock<std::mutex> lock(mutex_);
		while(!condition_variable_.wait_for(lock, timeout*1ms,  [&blazing_timer, this] {
				bool done_waiting = this->finished.load(std::memory_order_seq_cst);
				if (!done_waiting && blazing_timer.elapsed_time() > 59000 && this->log_timeout){
                    std::shared_ptr<spdlog::logger> logger = spdlog::get("batch_logger");
					if(logger) {
						logger->warn("|||{info}|{duration}||||",
											"info"_a="WaitingQueue " + this->queue_name + " get_all_or_wait timed out",
											"duration"_a=blazing_timer.elapsed_time());
					}
				}
				return done_waiting;
			})){}
		return get_all_unsafe();
	}

	/**
	* A function that returns a unique_lock using the WaitingQueue's mutex.
	* @return A unique_lock
	*/
	std::unique_lock<std::mutex> lock(){
		std::unique_lock<std::mutex> lock(mutex_);
		return std::move(lock);
	}

	/**
	* Get all messagse in the WaitingQueue without locking.
	* @return A vector with all the messages in the WaitingQueue.
	*/
	std::vector<message_ptr> get_all_unsafe() {
		std::vector<message_ptr> messages;
		for(message_ptr & it : message_queue_) {
			messages.emplace_back(std::move(it));
		}
		message_queue_.clear();
		return messages;
	}

	/**
	* Put a vector of messages onto the WaitingQueue without locking.
	* @param messages A vector of messages that will be pushed into the WaitingQueue.
	*/
	void put_all_unsafe(std::vector<message_ptr> messages) {
		for(size_t i = 0; i < messages.size(); i++) {
			putWaitingQueue(std::move(messages[i]));
		}
	}


	void put_all(std::vector<message_ptr> messages){
		std::unique_lock<std::mutex> lock(mutex_);
		put_all_unsafe(std::move(messages));
		processed += messages.size();
		condition_variable_.notify_all();
	}
private:
	/**
	* Checks if the WaitingQueue is empty.
	* @return A bool indicating if the WaitingQueue is empty.
	*/
	bool empty() {
		return this->message_queue_.size() == 0;
	}
	/**
	* Adds a message to the WaitingQueue
	* @param item The message to add to the WaitingQueue.
	*/

	void putWaitingQueue(message_ptr item) { message_queue_.emplace_back(std::move(item)); }

private:
	std::mutex mutex_; /**< This mutex is used for making access to the
											WaitingQueue thread-safe. */
	std::deque<message_ptr> message_queue_; /**< */
	std::condition_variable condition_variable_; /**< Used to notify waiting
																								functions*/
	int processed = 0; /**< Count of messages added to the WaitingQueue. */

	std::string queue_name;
	std::atomic<bool> finished; /**< Indicates if this WaitingQueue is finished. */
	int timeout; /**< timeout period in ms used by the wait_for to log that the condition_variable has been waiting for a long time. */
	bool log_timeout; /**< Whether or not to log when a timeout accurred. */
};

}  // namespace cache


} // namespace ral
