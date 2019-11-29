#include "Library/Logging/TcpOutput.h"
#include <stdlib.h>

namespace Library {
namespace Logging {
TcpOutput::TcpOutput() : isActive{false}, isReady{false}, isDataReadyToSend{false} {
	// srand(time(NULL));
	// nodeInd = (unsigned int)((rand() + 1) * 1000); // times 1000 so that it is obvious that its not an actual nodeInd
	// but a temporary unique id
}

TcpOutput::~TcpOutput() {
	{
		std::unique_lock<std::mutex> lock(mutex);
		isReady = true;
		isActive = false;
		condition.notify_one();
	}

	if(thread.joinable()) {
		thread.join();
	}
}

void TcpOutput::setMaxBufferSize(int value) {
	maxBufferSize = value;
	buffer.reserve(value);
}

void TcpOutput::setWaitTime(std::chrono::milliseconds && value) { waitTime = value; }

void TcpOutput::setWaitTime(const std::chrono::milliseconds & value) { waitTime = value; }

void TcpOutput::setSocket(std::shared_ptr<Library::Network::GenericSocket> & value) { socket = value; }

void TcpOutput::start() {
	isActive = true;
	thread = std::thread(&TcpOutput::doOnConsumer, this);
}

void TcpOutput::stop() {
	std::unique_lock<std::mutex> lock(mutex);
	isActive = false;
}

void TcpOutput::flush(std::string && log) { doOnProducer(log); }

void TcpOutput::flush(const std::string & log) { doOnProducer(log); }

void TcpOutput::flush(
	const int nodeInd, const std::string & datetime, const std::string & level, const std::string & log) {
	doOnProducer(datetime + "|" + std::to_string(nodeInd) + "|" + level + "|" + log);
}

// void TcpOutput::setNodeIdentifier(const unsigned int nodeInd) {

// 	std::string message = "Node index " + std::to_string(nodeInd) + " was using temporary node index identifier " +
// std::to_string(this->nodeInd);

// 	this->nodeInd = nodeInd;
// 	doOnProducer(message);
// }

void TcpOutput::doOnProducer(const std::string & log) {
	std::unique_lock<std::mutex> lock(mutex);

	// deque.push_back("[" + std::to_string(nodeInd) + "]" + log + "\n");
	deque.push_back(log + "\n");
	isReady = true;
	condition.notify_one();
}

void TcpOutput::doOnConsumer() {
	while(true) {
		{
			std::unique_lock<std::mutex> lock(mutex);

			if(deque.empty() && buffer.empty()) {
				isReady = false;
				while(!isReady) {
					condition.wait(lock);
				}
			} else if(deque.empty() && !buffer.empty()) {
				isReady = false;
				auto time = std::chrono::steady_clock::now() + waitTime;
				while(!isReady) {
					auto status = condition.wait_until(lock, time);
					if(status == std::cv_status::timeout) {
						isDataReadyToSend = true;
						break;
					}
				}
			}

			if(!isActive) {
				deque.clear();
				buffer.clear();
				break;
			}

			processLogData();
		}

		sendLogData();
	}
}

void TcpOutput::sendLogData() {
	if(isDataReadyToSend.exchange(false)) {
		socket->write(buffer);
		buffer.clear();
	}
}

void TcpOutput::processLogData() {
	int counter = buffer.length();
	while(!deque.empty()) {
		auto & log = deque.front();
		counter += log.length();
		if(maxBufferSize < counter) {
			isDataReadyToSend = true;
			break;
		}
		buffer += log;
		deque.pop_front();
	}
}
}  // namespace Logging
}  // namespace Library
